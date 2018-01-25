#!/bin/bash
export PATH=/home/tops/bin/:${PATH}
export temppath=$1
cd $temppath/rpm
sed -i  "s/^Release:.*$/Release: "$4"/" $2.spec
sed -i  "s/^Version:.*$/Version: "$3"/" $2.spec
export TAGS=TAG:`svn info|grep "URL"|cut -d ":" -f 2-|sed "s/^ //g"|awk -F "trunk|tags|branche" '{print $1}'`tags/$2_A_`echo $3|tr "." "_"`_$4
sed -i  "s#%description#%description \n $TAGS#g"  $2.spec
/usr/local/bin/rpm_create -p /home/admin -v $3 -r $4 $2.spec -k
mv `find . -name $2-$3-$4*rpm`  .
