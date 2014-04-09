#!/bin/bash -e
BUILD_ARCH=noarch
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..
BASEDIR=$PWD

ESMAIN=$1
ESSECOND=$2
CMSSWBASE=$3
DBPARAMS=$4
NTHREADS=$5

if [ -f $SCRIPTDIR/paramcache ];
then
readarray lines < $SCRIPTDIR/paramcache
lines[0]=`echo -n ${lines[0]} | tr -d "\n"`
lines[1]=`echo -n ${lines[1]} | tr -d "\n"`
lines[2]=`echo -n ${lines[2]} | tr -d "\n"`
lines[3]=`echo -n ${lines[3]} | tr -d "\n"`
lines[4]=`echo -n ${lines[4]} | tr -d "\n"`
lines[5]=`echo -n ${lines[5]} | tr -d "\n"`
else
lines=("" "" "" "" "")
fi

echo "ES server containg common run index (press enter for \"${lines[0]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[0]=$readin
fi
echo "ES tribe server (press enter for \"${lines[1]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[1]=$readin
fi

echo "CMSSW base (press enter for \"${lines[2]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[2]=$readin
fi

echo "HWCFG DB server (press enter for \"${lines[3]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[3]=$readin
fi

echo "HWCFG DB password (press enter for: \"${lines[4]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[4]=$readin
fi


echo "job username (press enter for: \"${lines[5]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[5]=$readin
fi

params="${lines[0]} ${lines[1]} ${lines[2]} ${lines[3]} ${lines[4]} ${lines[5]} 1"
#echo $params

#write cache
if [ -f $SCRIPTDIR/paramcache ];
then
rm -rf -f $SCRIPTDIR/paramcache
fi
echo ${lines[0]} >> $SCRIPTDIR/paramcache
echo ${lines[1]} >> $SCRIPTDIR/paramcache
echo ${lines[2]} >> $SCRIPTDIR/paramcache
echo ${lines[3]} >> $SCRIPTDIR/paramcache
echo ${lines[4]} >> $SCRIPTDIR/paramcache
echo ${lines[5]} >> $SCRIPTDIR/paramcache
chmod 500 $SCRIPTDIR/paramcache
# create a build area

echo "removing old build area"
rm -rf /tmp/fffmeta-build-tmp
echo "creating new build area"
mkdir  /tmp/fffmeta-build-tmp
ls
cd     /tmp/fffmeta-build-tmp
mkdir BUILD
mkdir RPMS
TOPDIR=$PWD
echo "working in $PWD"
ls

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > fffmeta.spec <<EOF
Name: fffmeta
Version: 1.3.0
Release: 0
Summary: hlt daemon
License: gpl
Group: Hacks
Packager: zee-sub-wun
Source: none
%define _topdir $TOPDIR
BuildArch: $BUILD_ARCH
AutoReqProv: no
Requires:elasticsearch > 1.0.2, hltd > 1.3.0, cx_Oracle >= 5.1.2

Provides:/usr/share/fff/configurefff.sh
Provides:/usr/share/fff/setupmachine.py

Provides:/usr/share/fff/elasticsearch.yml
Provides:/usr/share/fff/elasticsearch
Provides:/usr/share/fff/hltd.conf

%description
fffmeta configuration setup package

%prep
%build

%install
rm -rf \$RPM_BUILD_ROOT
mkdir -p \$RPM_BUILD_ROOT
%__install -d "%{buildroot}/usr/share/fff"

mkdir -p usr/share/fff
cp $BASEDIR/python/setupmachine.py %{buildroot}/usr/share/fff/setupmachine.py
echo "#!/bin/bash" > %{buildroot}/usr/share/fff/configurefff.sh
echo python2.6 /usr/share/fff/setupmachine.py elasticsearch,hltd $params >> %{buildroot}/usr/share/fff/configurefff.sh 

%files
%defattr(-, root, root, -)
#/usr/share/fff
%attr( 755 ,root, root) /usr/share/fff/setupmachine.py
%attr( 755 ,root, root) /usr/share/fff/setupmachine.pyc
%attr( 755 ,root, root) /usr/share/fff/setupmachine.pyo
%attr( 700 ,root, root) /usr/share/fff/configurefff.sh

%post
echo "post install trigger"

%triggerin -- elasticsearch
echo "triggered on elasticsearch update or install"
python2.6 /usr/share/fff/setupmachine.py elasticsearch $params
/sbin/service elasticsearch restart

%triggerin -- hltd
echo "triggered on hltd update or install"
python2.6 /usr/share/fff/setupmachine.py hltd $params
killall hltd
/sbin/service hltd restart

%preun
python2.6 /usr/share/fff/setupmachine.py restore

EOF

#mkdir -p RPMBUILD/{RPMS/{noarch},SPECS,BUILD,SOURCES,SRPMS}
rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta.spec
#rm -rf patch-cmssw-tmp

