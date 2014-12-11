/***************************************************************************\
 * slurm-spank-x11.c - SLURM SPANK X11 plugin
 ***************************************************************************
 * Copyright  CEA/DAM/DIF (2008)
 *
 * Written by Matthieu Hautreux <matthieu.hautreux@cea.fr>
 *
 * This file is part of slurm-spank-x11, a SLURM SPANK Plugin aiming at 
 * providing access to X11 display through tunneling on SLURM execution
 * nodes using OpenSSH.
 *
 * slurm-spank-x11 is free software; you can redistribute it and/or modify 
 * it under the terms of the GNU General Public License as published by the 
 * Free Software Foundation; either version 2 of the License, or (at your 
 * option) any later version.
 *
 * slurm-spank-x11 is distributed in the hope that it will be useful, but 
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License 
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with slurm-spank-x11; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
\***************************************************************************/
/* Note: To compile: gcc -fPIC -shared -o x11 slurm-spank-x11-plug.c */
#include <sys/resource.h>
#include <sys/types.h>
#include <pwd.h>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include <stdint.h>

#include <slurm/slurm.h>
#include <slurm/spank.h>

#ifndef X11_LIBEXEC_PROG
#define X11_LIBEXEC_PROG         "/usr/libexec/slurm-spank-x11"
#endif

#define SPANK_X11_ENVVAR         "SLURM_SPANK_X11" 

#define X11_MODE_NONE    0
#define X11_MODE_FIRST   1
#define X11_MODE_LAST    2
#define X11_MODE_ALL     3
#define X11_MODE_BATCH   4

#define INFO  slurm_debug
#define DEBUG slurm_debug
#define ERROR slurm_error

static int x11_mode = X11_MODE_NONE ;

static char* ssh_cmd = NULL;
static char* ssh_args = NULL;
static char* helpertask_args = NULL ;

/* 
 * can be used to adapt the ssh parameters to use to 
 * set up the ssh tunnel
 *
 * this can be overriden by ssh_cmd= and ssh_args= 
 * spank plugin conf args 
 */
#define DEFAULT_SSH_CMD "ssh"
#define DEFAULT_SSH_ARGS ""

/*
 * can be used to add trailing options to the command executed to 
 * set up the ssh tunnel
 *
 * this can be overriden by helpertask_args= spank plugin conf arg
 */
#define DEFAULT_HELPERTASK_ARGS ""

/*
 * All spank plugins must define this macro for the SLURM plugin loader.
 */
SPANK_PLUGIN(x11, 1);

/*
 * Implement a local version of popen that ensure that the command is 
 * ran with real and saved set-user-id uid/git set to effective uid/gid
 */
static FILE *xpopen(const char *command, const char *mode)
{
	int pid;
	int pep[2];          /* pipe endpoints */
	int p_end, c_end;    /* identify parent and child endpoints */

	uid_t euid;
	gid_t egid;

	/* identify process to pipe endpoints based on requested mode (r|w) */
	if ( *mode == 'r' ) {
		p_end = 0;
		c_end = 1 ;
	} else if ( *mode == 'w' ) {
		p_end = 1;
		c_end = 0 ;
	} else
		return NULL ;
	
	/* create the pipe */
	if ( pipe(pep) < 0 )
		return NULL;

	/* create a child task to run the command and get the 
	 * associated FILE* in the parent process */
	switch( pid = fork() )
	{

	case -1: 
		ERROR("xopen: unable to fork child task");
		return NULL;
	case 0:
		/* child process closes the parent endpoint 
		 * and redirect the pipe to either the stdin or 
		 * stdout fd (0|1) */
		if (close(pep[p_end]) == -1)
			exit(1);
		if (dup2(pep[c_end],c_end) == -1)
			exit(1);
		if (close(pep[c_end]) == -1)
			exit(1);
		
		/* change real/saved_set-user-id uid and gid to match the effective one
		 * without that the execl of /bin/sh would automatically revert to
		 * an execution using the real uid/gid. That is a security issue that we
		 * have to avoid.
		 */
		euid = geteuid();
		egid = getegid();
		if (setresgid(egid,-1,egid) || setresuid(euid,-1,euid))
			exit(2);
	      
		/* execute the  provided command */
		execl( "/bin/sh", "sh", "-c", command, NULL );
		exit(1);

	default:
		/* parent process closes the child endpoint and return 
		 * the file descriptor to use in either r or w mode */
		if ( close(pep[c_end]) == -1 )
			return NULL;
		return fdopen(pep[p_end], mode);
	}
} 

/*
 *  Provide a --x11=first|last|all option to srun:
 */
static int _x11_opt_process (int val, const char *optarg, int remote);


struct spank_option spank_opts[] =
{
	{ "x11", "[batch|first|last|all]", 
	  "Export x11 display on first|last|all allocated node(s)", 2, 0,
	  (spank_opt_cb_f) _x11_opt_process
	},
	SPANK_OPTIONS_TABLE_END
};


/*
 *
 * SLURM SPANK API SLURM SPANK API SLURM SPANK API SLURM SPANK API
 *
 * SLURM SPANK API SLURM SPANK API SLURM SPANK API SLURM SPANK API
 *
 */
int
slurm_spank_init (spank_t sp, int ac, char *av[])
{
        spank_option_register(sp,spank_opts);
	x11_mode = _spank_x11_get_mode(sp,ac,av);

	return 0;
}

/*
 * srun call, the client node connects the allocated node(s)
 */
int slurm_spank_local_user_init (spank_t sp, int ac, char **av)
{
	int status;

	uint32_t jobid;
	uint32_t stepid;
	job_info_msg_t * job_buffer_ptr;
	job_info_t* job_ptr;

	/* only handle interactive usage */
	if ( x11_mode == X11_MODE_NONE || 
	     x11_mode == X11_MODE_BATCH )
		return 0;

	/* check DISPLAY value */
	if ( getenv("DISPLAY") == NULL ) {
		ERROR("x11: no local DISPLAY defined, skipping");
		return 0;
	}

	/* get job id */
	if ( spank_get_item (sp, S_JOB_ID, &jobid)
	     != ESPANK_SUCCESS ) {
		status = -1;
		goto exit;
	}
	
	/* get job step id */
	if ( spank_get_item (sp, S_JOB_STEPID, &stepid)
	     != ESPANK_SUCCESS ) {
		status = -1;
		goto exit;
	}
	
	/* get job infos */
	status = slurm_load_job(&job_buffer_ptr,jobid,SHOW_ALL);
	if ( status != 0 ) {
		ERROR("x11: unable to get job infos");
		status = -3;
		goto exit;
	}

	/* check infos validity  */
	if ( job_buffer_ptr->record_count != 1 ) {
		ERROR("x11: job infos are invalid");
		status = -4;
		goto clean_exit;
	}
	job_ptr = job_buffer_ptr->job_array;

	/* check allocated nodes var */
	if ( job_ptr->nodes == NULL ) {
		ERROR("x11: job has no allocated nodes defined");
		status = -5;
		goto clean_exit;
	}

	/* connect required nodes */
	status = _x11_connect_nodes(job_ptr->nodes,jobid,stepid);

clean_exit:
	slurm_free_job_info_msg(job_buffer_ptr);

exit:
	return status;
}


int _x11_init_remote_inter(spank_t sp,uint32_t jobid,uint32_t stepid)
{
	FILE* f;
	int status = -1;
	char* cmd_pattern= X11_LIBEXEC_PROG " -i %u.%u -g";
	char* cmd;
	size_t cmd_length;
	char display[256];
        
	/* build slum-spank-x11 command to retrieve connected DISPLAY to use */
	cmd_length = strlen(cmd_pattern) + 128 ;
	cmd = (char*) malloc(cmd_length*sizeof(char));
	if ( cmd == NULL || 
	     snprintf(cmd,cmd_length,cmd_pattern,jobid,stepid) >= cmd_length ) {
		ERROR("x11: error while building cmd");
		status = -2;
	}
	else {
	        /* execute the command to retrieve the DISPLAY value to use */
		f = popen(cmd,"r");
		if ( f != NULL ) {
			if ( fscanf(f,"%255s\n",display) == 1 ) {
				if ( spank_setenv(sp,"DISPLAY",display,1) 
				     != ESPANK_SUCCESS ) {
					ERROR("x11: unable "
						    "to set DISPLAY in env");
					status = -5;
				}
				else {
					INFO("x11: now using DISPLAY=%s",
						   display);
					status = 0;
				}
			}
			else {
				ERROR("x11: unable to read DISPLAY value");
				status = -4;
			}
			pclose(f);
		}
		else {
		        ERROR("x11: unable to exec get cmd '%s'",cmd);
			status = -3;
		}
		free(cmd);
	}
	
	return status;
}

int _x11_init_remote_batch(spank_t sp,uint32_t jobid,uint32_t stepid)
{
	int status;

	FILE* f;
	char localhost[256];
	char* cmd_pattern= X11_LIBEXEC_PROG " -u %s -s \"%s\" -o \"%s\" -f %s -d %s -t %s -i %u.%u -cwg %s &";
	char* cmd;
	size_t cmd_length;
	char display[256];
	
        struct passwd user_pwent;
        struct passwd *p_pwent;
        size_t pwent_buffer_length = sysconf(_SC_GETPW_R_SIZE_MAX);
        char pwent_buffer[pwent_buffer_length];
        
	job_info_msg_t * job_buffer_ptr;
	job_info_t* job_ptr;
	
	/*
	 * get current hostname 
	 */
	if ( gethostname(localhost,256) != 0 ) {
		status = -20;
		goto exit;
	}
	
	/*
	 * the batch script inherits the DISPLAY value of the 
	 * submission command. We will use it on the allocation node
	 * for proper establishment of a working X11 ssh tunnel
	 */
	if ( spank_getenv(sp,"DISPLAY",display,256) != ESPANK_SUCCESS ) {
		ERROR("x11: unable to read batch step "
			    "inherited DISPLAY value");
		status = -1;
		goto exit;
	}
	
	/* get job infos */
	status = slurm_load_job(&job_buffer_ptr,jobid,SHOW_ALL);
	if ( status != 0 ) {
		ERROR("x11: unable to get job infos");
		status = -3;
		goto exit;
	}
	
	/* check infos validity  */
	if ( job_buffer_ptr->record_count != 1 ) {
		ERROR("x11: job infos are invalid");
		status = -4;
		goto clean_exit;
	}
	job_ptr = job_buffer_ptr->job_array;
	
	/* get user name */
	status = getpwuid_r(job_ptr->user_id,&user_pwent,pwent_buffer,
			    pwent_buffer_length,&p_pwent) ;
        if (status) {
                error("x11: unable to get username for uid=%u : %s",job_ptr->user_id,
		      strerror(status)) ;
		status = -10;
		goto clean_exit;
        }
	
	/* 
	 * build the command line that will be used to forward the 
	 * alloc node X11 tunnel
	 */
	cmd_length = strlen(cmd_pattern) + 128 ;
	cmd = (char*) malloc(cmd_length*sizeof(char));
	if ( cmd == NULL ||
	     snprintf(cmd,cmd_length,cmd_pattern,user_pwent.pw_name,
		      (ssh_cmd == NULL) ? DEFAULT_SSH_CMD : ssh_cmd,
		      (ssh_args == NULL) ? DEFAULT_SSH_ARGS : ssh_args,
		      job_ptr->alloc_node,display,localhost,jobid,stepid,
		      (helpertask_args == NULL) ? DEFAULT_HELPERTASK_ARGS : helpertask_args) >= cmd_length ) {
		ERROR("x11: error while building cmd");
		status = -2;
	}
	else {
		INFO("x11: batch mode : executing %s",cmd);
	        /* execute the command to retrieve the DISPLAY value to use */
		f = xpopen(cmd,"r");
		if ( f != NULL ) {
			if ( fscanf(f,"%255s",display) == 1 ) {
				if ( spank_setenv(sp,"DISPLAY",display,1)
				     != ESPANK_SUCCESS ) {
					ERROR("x11: unable to set DISPLAY"
						    " in job env");
					status = -5;
				}
				else {
					INFO("x11: now using DISPLAY=%s",
						   display);
					status=0;
				}
			}
			else {
				ERROR("x11: unable to get a DISPLAY value");
				status = -6;
			}
			pclose(f);
		}
		else {
		        ERROR("x11: unable to exec get cmd '%s'",cmd);
			status = -3;
		}
	}

	if ( cmd != NULL )
		free(cmd);

clean_exit:
	slurm_free_job_info_msg(job_buffer_ptr);
	
exit:
	return status;
}

/*
 * in remote mode, read DISPLAY file content and set its value in job's DISPLAY
 * environment variable 
 */
int slurm_spank_user_init (spank_t sp, int ac, char **av)
{
	int status=-1;
	int do_init=0;
	uint32_t jobid;
	uint32_t stepid;
	uint32_t nnodes;
	uint32_t nodeid; 

	if ( x11_mode == X11_MODE_NONE )
		return 0;

	/* get job id */
	if ( spank_get_item (sp, S_JOB_ID, &jobid) != ESPANK_SUCCESS )
		return status;

	/* get job step id */
	if ( spank_get_item (sp, S_JOB_STEPID, &stepid) != ESPANK_SUCCESS )
	        return status;

	if ( stepid == SLURM_BATCH_SCRIPT && x11_mode == X11_MODE_BATCH ) {
		return _x11_init_remote_batch(sp,jobid,stepid);
	}
	else if ( x11_mode != X11_MODE_BATCH ) {

		/* get the number of nodes */
		if ( spank_get_item (sp, S_JOB_NNODES, &nnodes) != ESPANK_SUCCESS )
			return status;
		
		/* get the local node ID */
		if ( spank_get_item (sp, S_JOB_NODEID, &nodeid) != ESPANK_SUCCESS )
			return status;

		/* test if the local node has to go further */
		switch ( x11_mode ) {
		case X11_MODE_FIRST :
			if ( nodeid == 0 ) {
				do_init = 1;
			}
			break;
		case X11_MODE_LAST :
			if ( nodeid == (nnodes - 1) ) {
				do_init = 1;
			}
			break;
		case X11_MODE_ALL :
			do_init = 1;
			break;
		default :
			break;
		}
		
		/* do the initialization of the X11 export if requested */
		if ( do_init == 1 )
			return _x11_init_remote_inter(sp,jobid,stepid);
		else
			return 0;
	}

}

/*
 * in remote mode, remove DISPLAY file in order to stop
 * ssh -X process initialized by the client
 */
int slurm_spank_exit (spank_t sp, int ac, char **av)
{
	uint32_t jobid;
	uint32_t stepid;

	FILE* f;
	char* expc_pattern= X11_LIBEXEC_PROG " -i %u.%u -r 2>/dev/null";
	char* expc_cmd;
	size_t expc_length;
	
	/* noting to do in local mode */
	if (!spank_remote (sp))
		return 0;

	/* get job id */
	if ( spank_get_item (sp, S_JOB_ID, &jobid) != ESPANK_SUCCESS )
		return -1;

	/* get job step id */
	if ( spank_get_item (sp, S_JOB_STEPID, &stepid) != ESPANK_SUCCESS )
		return -1;
	
	/* remove DISPLAY reference */
	expc_length = strlen(expc_pattern) + 128 ;
	expc_cmd = (char*) malloc(expc_length*sizeof(char));
	if ( expc_cmd != NULL && 
	     ( snprintf(expc_cmd,expc_length,expc_pattern,jobid,stepid)
	       >= expc_length )	) {
		ERROR("x11: error while creating remove reference cmd");
	}
	else {
		f = xpopen(expc_cmd,"r");
		if ( f == NULL ) {
			ERROR("x11: unable to exec remove"
				    " cmd '%s'",expc_cmd);
		}
		else
			pclose(f);		
	}
	if ( expc_cmd != NULL )
		free(expc_cmd);
	
	return 0;
}

static int _x11_opt_process (int val, const char *optarg, int remote)
{
	if (optarg == NULL) {
		x11_mode = X11_MODE_FIRST;
		return (0);
	}

	if ( strncmp(optarg,"first",6)==0 ) {
		x11_mode = X11_MODE_FIRST;
	}
	else if ( strncmp(optarg,"last",5)==0 ) {
		x11_mode = X11_MODE_LAST;
	}
	else if ( strncmp(optarg,"all",4)==0 ) {
		x11_mode = X11_MODE_ALL;
	}
	else if ( strncmp(optarg,"batch",5)==0 ) {
		x11_mode = X11_MODE_BATCH;
	}

	if ( x11_mode == X11_MODE_NONE ) {
		ERROR ("Bad value for --x11: %s", optarg);
		return (-1);
	}

	return (0);
}

int _connect_node (char* node,uint32_t jobid,uint32_t stepid)
{
	int status = -1;
	
	FILE* f;
	char display[256];
	char* expc_pattern= X11_LIBEXEC_PROG " -t %s -i %u.%u -cgw -s \"%s\" -o \"%s\" 2>/dev/null %s &";
	char* expc_cmd;
	size_t expc_length;
	
	expc_length = strlen(expc_pattern) + strlen(node) + 128 +
		strlen((ssh_cmd == NULL) ? DEFAULT_SSH_CMD : ssh_cmd)  +
		strlen((ssh_args == NULL) ? DEFAULT_SSH_ARGS : ssh_args) +
		strlen((helpertask_args == NULL) ?
		       DEFAULT_HELPERTASK_ARGS : helpertask_args) ;
	expc_cmd = (char*) malloc(expc_length*sizeof(char));
	if ( expc_cmd != NULL ) {
		snprintf(expc_cmd,expc_length,expc_pattern,node,jobid,stepid,
			 (ssh_cmd == NULL) ? DEFAULT_SSH_CMD : ssh_cmd,
			 (ssh_args == NULL) ? DEFAULT_SSH_ARGS : ssh_args,
			 (helpertask_args == NULL) ? 
			 DEFAULT_HELPERTASK_ARGS : helpertask_args );
		INFO("x11: interactive mode : executing %s",expc_cmd);		
		f = popen(expc_cmd,"r");
		if ( fscanf(f,"%255s",display) != 1 )
			ERROR("x11: unable to connect node %s",node);
		else {
			INFO("x11: DISPLAY=%s on node %s",display,node);
			status = 0;
		}
		pclose(f);
		free(expc_cmd);
	}
	
	return status;
}

int _x11_connect_nodes (char* nodes,uint32_t jobid,uint32_t stepid)
{
	char* host;
	hostlist_t hlist;
	int n=0;
	int i;
	
	/* count allocated nodes... */
	hlist = slurm_hostlist_create(nodes);
	n=-1;
	do {
		n++;
		host = slurm_hostlist_shift(hlist);
	}
	while ( host != NULL ) ;
	slurm_hostlist_destroy(hlist);
	
	/* do the export stuff */
	hlist = slurm_hostlist_create(nodes);
	for (i=0; i < n; i++ ) {
		host = slurm_hostlist_shift(hlist);
		switch ( x11_mode ) {
			
		case X11_MODE_FIRST :
			if ( i == 0 ) {
				_connect_node(host,jobid,stepid);
			}
			break;

		case X11_MODE_LAST :
			if ( i == (n - 1) ) {
				_connect_node(host,jobid,stepid);
			}
			break;
			
		case X11_MODE_ALL :
			_connect_node(host,jobid,stepid);
			break;
			
		default :
			break;
		}
	}
	slurm_hostlist_destroy(hlist);

	return 0;
}


int
_spank_x11_get_mode(spank_t sp, int ac, char *av[])
{
	int i;
	char* elt;
	char* p;
        int fstatus;
	char spank_x11_env[6];

	char* envval=NULL;

	/* get configuration line parameters, replacing '|' with ' ' */
        for (i = 0; i < ac; i++) {
                elt = av[i];
                if ( strncmp(elt,"ssh_cmd=",8) == 0 ) {
                        ssh_cmd=strdup(elt+8);
			p = ssh_cmd;
			while ( p != NULL && *p != '\0' ) {
				if ( *p == '|' )
					*p= ' ';
				p++;
			}
                }
                else if ( strncmp(elt,"ssh_args=",9) == 0 ) {
                        ssh_args=strdup(elt+9);
			p = ssh_args;
			while ( p != NULL && *p != '\0' ) {
				if ( *p == '|' )
					*p= ' ';
				p++;
			}
                }
                else if ( strncmp(elt,"helpertask_args=",16) == 0 ) {
                        helpertask_args=strdup(elt+16);
			p = helpertask_args;
			while ( p != NULL && *p != '\0' ) {
				if ( *p == '|' )
					*p= ' ';
				p++;
			}
                }
        }

	/* read env configuration variable */
	if (spank_remote (sp)) {
		fstatus = spank_getenv(sp,SPANK_X11_ENVVAR,
				       spank_x11_env,6);
		if ( fstatus == 0 ) {
			spank_x11_env[5]='\0';
			envval=spank_x11_env;
		}
	}
	else {
		envval = getenv(SPANK_X11_ENVVAR);
	}
	/* if env variable is set, use it */
	if ( envval != NULL ) {
		/* check env var value (can be yes|no|done)*/
		if ( strncmp(envval,"first",5) == 0 ) {
			return X11_MODE_FIRST ;
		}
		else if ( strncmp(envval,"last",4) == 0 ) {
			return X11_MODE_LAST ;
		}
		else if ( strncmp(envval,"all",3) == 0 ) {
			return X11_MODE_ALL ;
		}
		else if ( strncmp(envval,"batch",5) == 0 ) {
			return X11_MODE_BATCH ;
		}
		else
			return X11_MODE_NONE ;
	}
	else {
                /* no env variable defined, return command line */
                /* or configuration file auks flag */
                return x11_mode ;
        }

}
