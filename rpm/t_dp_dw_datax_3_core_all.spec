Name: t_dp_dw_datax_3_core_all
Packager:xiafei.qiuxf
Version:201607221827
Release: %(echo $RELEASE)%{?dist}

Summary: datax 3 core
URL: http://gitlab.alibaba-inc.com/datax/datax
Group: t_dp
License: Commercial
BuildArch: noarch


%define __os_install_post %{nil}

%description
CodeUrl: http://gitlab.alibaba-inc.com/datax/datax
datax core
%{_svn_path}
%{_svn_revision}

%define _prefix /home/admin/datax3
%define _plugin6 /home/admin/datax3/plugin_%{version}_%{release}
%define _lib6 /home/admin/datax3/lib_%{version}_%{release}

%prep
export LANG=zh_CN.UTF-8

%pre
grep -q "^cug-tbdp:" /etc/group &>/dev/null || groupadd -g 508 cug-tbdp &>/dev/null || true
grep -q "^taobao:" /etc/passwd &>/dev/null || useradd -u 503 -g cug-tbdp taobao &>/dev/null || true
if [ -d %{_prefix}/log ]; then
    find %{_prefix}/log -type f -mtime +7 -exec rm -rf {} \;
    find %{_prefix}/log -type d -empty -mtime +7 -exec rm -rf {} \;
    find %{_prefix}/log_perf -type f -mtime +7 -exec rm -rf {} \;
    find %{_prefix}/log_perf -type d -empty -mtime +7 -exec rm -rf {} \;
fi

mkdir -p %{_plugin6}
mkdir -p %{_lib6}

%build
cd ${OLDPWD}/../

export MAVEN_OPTS="-Xms256m -Xmx1024m -XX:MaxPermSize=128m"
#/home/ads/tools/apache-maven-3.0.3/bin/
mvn clean package -DskipTests assembly:assembly

%install

mkdir -p .%{_plugin6}
mkdir -p .%{_lib6}
cp -rf $OLDPWD/../target/datax/datax/bin .%{_prefix}/.
cp -rf $OLDPWD/../target/datax/datax/conf .%{_prefix}/.
cp -rf $OLDPWD/../target/datax/datax/job .%{_prefix}/.
cp -rf $OLDPWD/../target/datax/datax/script .%{_prefix}/.
cp -rf $OLDPWD/../target/datax/datax/lib/* .%{_lib6}/.
cp -rf $OLDPWD/../target/datax/datax/plugin/* .%{_plugin6}/.

# make dir for hook
mkdir -p .%{_prefix}/hook
mkdir -p .%{_prefix}/tmp
mkdir -p .%{_prefix}/log
mkdir -p .%{_prefix}/log_perf
mkdir -p .%{_prefix}/local_storage

%post
chmod -R 0755 %{_prefix}/bin
chmod -R 0755 %{_prefix}/conf
chmod -R 0755 %{_prefix}/job
chmod -R 0755 %{_prefix}/script
chmod -R 0755 %{_prefix}/hook
chmod -R 0777 %{_prefix}/tmp
chmod -R 0755 %{_prefix}/log
chmod -R 0755 %{_prefix}/log_perf
chmod -R 0755 %{_prefix}/local_storage
chmod -R 0700 %{_prefix}/conf/.secret.properties



# 指定新目录
# 如果datax3 plugin是软连接，直接删除，并创建新的软链接
if [ -L %{_prefix}/plugin ]; then
    oldplugin=$(readlink %{_prefix}/plugin)
    rm -rf %{_prefix}/plugin
    ln -s %{_plugin6} %{_prefix}/plugin

    oldlib=`readlink %{_prefix}/lib`
    rm -rf %{_prefix}/lib
    ln -s %{_lib6} %{_prefix}/lib

    ## 解决--force
    if [ "${oldplugin}" != "%{_plugin6}" ];then
        rm -rf ${oldplugin}
        rm -rf ${oldlib}
    fi

elif [ -d %{_prefix}/plugin ]; then
    mv %{_prefix}/plugin %{_prefix}/plugin_bak_rpm
    mv %{_prefix}/lib %{_prefix}/lib_bak_rpm

    ln -s %{_plugin6} %{_prefix}/plugin
    ln -s %{_lib6} %{_prefix}/lib

    rm -rf %{_prefix}/plugin_bak_rpm
    rm -rf %{_prefix}/lib_bak_rpm
else
    ln -s %{_lib6} %{_prefix}/lib
    ln -s %{_plugin6} %{_prefix}/plugin
fi

chown -h admin %{_prefix}/plugin
chown -h admin %{_prefix}/lib

chgrp -h cug-tbdp %{_prefix}/plugin
chgrp -h cug-tbdp %{_prefix}/lib

%files
%defattr(755,admin,cug-tbdp)
%config(noreplace) %{_prefix}/conf/core.json
%config(noreplace) %{_prefix}/conf/logback.xml
%config(noreplace) %{_prefix}/conf/.secret.properties

%{_prefix}
