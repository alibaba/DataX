Name: t_dp_dw_datax_3_hook_dqc
Packager:xiafei.qiuxf
Version:2014122220.3
Release: 1

Summary: datax 3 dqc hook
URL: http://gitlab.alibaba-inc.com/datax/datax
Group: t_dp
License: Commercial
BuildArch: noarch


%define __os_install_post %{nil}

%description
CodeUrl: http://gitlab.alibaba-inc.com/datax/datax
datax dqc hook
%{_svn_path}
%{_svn_revision}

%define _prefix /home/admin/datax3/hook/dqc

%prep
export LANG=zh_CN.UTF-8

%pre
grep -q "^cug-tbdp:" /etc/group &>/dev/null || groupadd -g 508 cug-tbdp &>/dev/null || true
grep -q "^taobao:" /etc/passwd &>/dev/null || useradd -u 503 -g cug-tbdp taobao &>/dev/null || true


%build
BASE_DIR="${OLDPWD}/../"

cd ${BASE_DIR}/

#/home/ads/tools/apache-maven-3.0.3/bin/
mvn install -N
#/home/ads/tools/apache-maven-3.0.3/bin/
mvn install -pl common -DskipTests
cd ${BASE_DIR}/dqchook
#/home/ads/tools/apache-maven-3.0.3/bin/
mvn clean package -DskipTests assembly:assembly
cd ${BASE_DIR}

%install
BASE_DIR="${OLDPWD}/../"
mkdir -p .%{_prefix}
cp -r ${BASE_DIR}/dqchook/target/datax/hook/dqc/* .%{_prefix}/

%post
chmod -R 0755 %{_prefix}


%files
%defattr(755,admin,cug-tbdp)
%config(noreplace) %{_prefix}/dqc.properties
%{_prefix}
