Summary: Slurm spank plugin for X11 display support
Name: slurm-spank-x11
Version: 0.2.5
Release: 1
License: GPL
Group: System Environment/Base
Source0: %{name}-%{version}.tar.gz
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

BuildRequires: slurm-devel
Requires: slurm

%description
x11 SLURM spank plugin enables to export X11 display on a part or all of the 
allocated nodes of SLURM jobs using openSSH.
If openSSH is properly configured, users can use this feature transparently, 
otherwise they will need to enter as many passwords as connections to establish.

%prep
%setup -q

%build
%{__cc} -g -o slurm-spank-x11 slurm-spank-x11.c
%{__cc} -g -shared -fPIC -o x11.so \
	-D"X11_LIBEXEC_PROG=\"%{_libexecdir}/%{name}\"" \
	slurm-spank-x11-plug.c

%install
rm -rf $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT%{_libdir}
mkdir -p $RPM_BUILD_ROOT%{_libdir}/slurm
mkdir -p $RPM_BUILD_ROOT%{_libexecdir}
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/slurm
mkdir -p $RPM_BUILD_ROOT%{_sysconfdir}/slurm/plugstack.conf.d
install -m 755 slurm-spank-x11 $RPM_BUILD_ROOT%{_libexecdir}
install -m 755 x11.so $RPM_BUILD_ROOT%{_libdir}/slurm
install -m 644 plugstack.conf $RPM_BUILD_ROOT%{_sysconfdir}/slurm/plugstack.conf.d/x11.conf.example

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_libexecdir}/slurm-spank-x11
%{_libdir}/slurm/x11.so
%config %{_sysconfdir}/slurm/plugstack.conf.d/x11.conf.example

%changelog
* Tue Nov 06 2012 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> -  0.2.5-1
- Correct a bug in --x11=first|last management, ensuring that only the
  targeted nodes performed X11 related actions
* Tue Nov 06 2012 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> -  0.2.4-1
- Move to the usage of xpopen instead of standard popen function
  (security fix)
* Mon Mar  5 2012 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> -  0.2.3-1
- First public version including GPL headers
* Mon Apr 12 2010 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> -  0.2.2-1
- first production release supporting batch mode
* Tue Dec 15 2009 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> -  0.2.1-1
- redesign helper task to enable batch mode
* Fri Nov 14 2008 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> - 
- 0.1.2 bug correction (x11 is no longer active by default)
* Tue Oct 21 2008 HAUTREUX Matthieu <matthieu.hautreux@cea.fr> - 
- Initial build.
