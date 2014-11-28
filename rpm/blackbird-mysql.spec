%define _unpackaged_files_terminate_build 0
%define name blackbird-mysql
%define version 0.1.1
%define unmangled_version 0.1.1
%define release 1

%define blackbird_conf_dir /etc/blackbird/conf.d

Summary: Get mysql information
Name: %{name}
Version: %{version}
Release: %{release}%{?dist}
Source0: %{name}-%{unmangled_version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: noarch
Vendor: makocchi <makocchi@gmail.com>
Packager: makocchi <makocchi@gmail.com>
Requires: blackbird
Requires: MySQL-python
Url: https://github.com/Vagrants/blackbird-mysql
BuildRequires:  python-devel

%description
Project Info
============

* Project Page: https://github.com/Vagrants/blackbird-mysql


%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install --skip-build --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
mkdir -p $RPM_BUILD_ROOT/%{blackbird_conf_dir}
cp -p mysql.cfg $RPM_BUILD_ROOT/%{blackbird_conf_dir}/mysql.cfg

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)

%changelog
* Fri Nov 28 2014 makochi <makocchi@gmail.com> - 0.1.1-1
- version up to 0.1.1

* Wed Jul 9 2014 makochi <makocchi@gmail.com> - 0.1.0-1
- first package
