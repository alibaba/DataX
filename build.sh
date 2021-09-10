ShellDir="$( cd "$( dirname "$0"  )" && pwd  )"
cd $ShellDir
mvn -U -pl starrockswriter -am clean package assembly:assembly -Dmaven.test.skip=true
rm -f starrockswriter.tar.gz
tar -czvf starrockswriter.tar.gz target/datax/datax/plugin/writer/starrockswriter
