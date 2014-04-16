#!/bin/bash -e
BUILD_ARCH=noarch
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $SCRIPTDIR/..
BASEDIR=$PWD

if [ -f $SCRIPTDIR/paramcache ];
then
  readarray lines < $SCRIPTDIR/paramcache
  for (( i=0; i < 10; i++ ))
  do
    lines[$i]=`echo -n ${lines[$i]} | tr -d "\n"`
  done
else
  for (( i=0; i < 10; i++ ))
  do
    lines[$i]=""
  done
fi

echo "ES server URL containg common run index (press enter for \"${lines[0]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[0]=$readin
fi
echo "ES tribe server hostname (press enter for \"${lines[1]}\"):"
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

echo "HWCFG DB SID (press enter for: \"${lines[4]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[4]=$readin
fi

echo "HWCFG DB username (press enter for: \"${lines[5]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[5]=$readin
fi

echo "HWCFG DB password (press enter for: \"${lines[6]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[6]=$readin
fi

echo "Equipment set (press enter for: \"${lines[7]}\") - type 'latest' to use latest eq set:"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[7]=$readin
fi

echo "job username (press enter for: \"${lines[8]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[8]=$readin
fi

echo "number of threads per process (press enter for: \"${lines[9]}\"):"
readin=""
read readin
if [ ${#readin} != "0" ]; then
lines[9]=$readin
fi

params=""
for (( i=0; i < 10; i++ ))
do
  params="$params ${lines[i]}"
done

#write cache
if [ -f $SCRIPTDIR/paramcache ];
then
rm -rf -f $SCRIPTDIR/paramcache
fi
for (( i=0; i < 10; i++ ))
do
  echo ${lines[$i]} >> $SCRIPTDIR/paramcache
done

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
Requires:elasticsearch >= 1.0.2, hltd > 1.3.0, cx_Oracle >= 5.1.2

Provides:/usr/share/fff/configurefff.sh
Provides:/usr/share/fff/setupmachine.py

Provides:/usr/share/fff/elasticsearch.yml
Provides:/usr/share/fff/elasticsearch
Provides:/usr/share/fff/hltd.conf
Provides:/etc/init.d/fffmeta

%description
fffmeta configuration setup package

%prep
%build

%install
rm -rf \$RPM_BUILD_ROOT
mkdir -p \$RPM_BUILD_ROOT
%__install -d "%{buildroot}/usr/share/fff"
%__install -d "%{buildroot}/etc/init.d"

mkdir -p usr/share/fff
cp $BASEDIR/python/setupmachine.py %{buildroot}/usr/share/fff/setupmachine.py
echo "#!/bin/bash" > %{buildroot}/usr/share/fff/configurefff.sh
echo python2.6 /usr/share/fff/setupmachine.py elasticsearch,hltd $params >> %{buildroot}/usr/share/fff/configurefff.sh 

#TODO:check if elasticsearch / hltd are already running and restart them if they are

mkdir -p etc/init.d/
echo "#!/bin/bash"                       >> %{buildroot}/etc/init.d/fffmeta
echo "#"                                 >> %{buildroot}/etc/init.d/fffmeta
echo "# chkconfig:   2345 79 21"         >> %{buildroot}/etc/init.d/fffmeta
echo "#"                                 >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"start\" ]; then"   >> %{buildroot}/etc/init.d/fffmeta
echo "  /usr/share/fff/configurefff.sh"  >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"restart\" ]; then" >> %{buildroot}/etc/init.d/fffmeta
echo "/usr/share/fff/configurefff.sh"    >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta
echo "if [ \\\$1 == \"status\" ]; then"  >> %{buildroot}/etc/init.d/fffmeta
echo "echo fffmeta does not have status" >> %{buildroot}/etc/init.d/fffmeta
echo "  exit 0"                          >> %{buildroot}/etc/init.d/fffmeta
echo "fi"                                >> %{buildroot}/etc/init.d/fffmeta


%files
%defattr(-, root, root, -)
#/usr/share/fff
%attr( 755 ,root, root) /usr/share/fff/setupmachine.py
%attr( 755 ,root, root) /usr/share/fff/setupmachine.pyc
%attr( 755 ,root, root) /usr/share/fff/setupmachine.pyo
%attr( 700 ,root, root) /usr/share/fff/configurefff.sh
%attr( 755 ,root, root) /etc/init.d/fffmeta

%post
#echo "post install trigger"
chkconfig fffmeta on

%triggerin -- elasticsearch
#echo "triggered on elasticsearch update or install"
python2.6 /usr/share/fff/setupmachine.py elasticsearch $params
/sbin/service elasticsearch restart
chkconfig elasticsearch on

%triggerin -- hltd
#echo "triggered on hltd update or install"
python2.6 /usr/share/fff/setupmachine.py hltd $params
killall hltd
/sbin/service hltd restart
chkconfig hltd on

%preun
chkconfig fffmeta off
chkconfig elasticsearch off
chkconfig hltd off

/sbin/service elasticsearch stop || true
#if ["\$?" == "0" ]; then
#echo success hltd
#else
#echo unsuccess hltd
#fi

/sbin/service hltd stop || true
#if ["\$?" == "0" ]; then
#echo success hltd
#else
#echo unsuccess hltd
#fi

python2.6 /usr/share/fff/setupmachine.py restore

#TODO:
#%verifyscript

EOF

rpmbuild --target noarch --define "_topdir `pwd`/RPMBUILD" -bb fffmeta.spec

