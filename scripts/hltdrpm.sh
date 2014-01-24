#!/bin/bash
# numArgs=$#
# if [ $numArgs -lt 4 ]; then
#     echo "Usage: patch-cmssw-build.sh CMSSW_X_Y_Z patchId {dev|pro|...} patchdir"
#     exit -1
# fi
# CMSSW_VERSION=$1            # the CMSSW version, as known to scram
# PATCH_ID=$2                 # an arbitrary tag which identifies the extra code (usually, "p1", "p2", ...)
# AREA=$3                     # "pro", "dev", etc...
# LOCAL_CODE_PATCHES_TOP=$4   # absolute path to the area where extra code to be compiled in can be found, equivalent to $CMSSW_BASE/src
alias python=python2.6
# set the RPM build architecture
#BUILD_ARCH=$(uname -i)      # "i386" for SLC4, "x86_64" for SLC5
BUILD_ARCH=x86_64
BASEDIR=$PWD

# create a build area

echo "removing old build area"
rm -rf /tmp/hltd-build-tmp
echo "creating new build area"
mkdir  /tmp/hltd-build-tmp
ls
cd     /tmp/hltd-build-tmp
TOPDIR=$PWD
ls 


echo "Moving files to their destination"
mkdir -p opt/hltd
mkdir -p etc/init.d
mkdir -p etc/appliance/resources/idle
mkdir -p etc/appliance/resources/online
mkdir -p etc/appliance/resources/offline
mkdir -p etc/appliance/resources/except
mkdir -p etc/appliance/resources/quarantined
mkdir -p usr/lib64/python2.6/site-packages
mkdir -p usr/lib64/python2.6/site-packages/pyelasticsearch
mkdir -p usr/lib64/python2.6/site-packages/simplejson
ls
cp -r $BASEDIR/python/hltd $TOPDIR/etc/init.d/hltd
cp -r $BASEDIR/* $TOPDIR/opt/hltd
cp -r $BASEDIR/etc/hltd.conf $TOPDIR/etc/
echo "working in $PWD"
ls opt/hltd


# build external libraries
#simplejson 3.3.1
cd opt/hltd/lib/simplejson-3.3.1/
./setup.py -q build
python - <<'EOF' 
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/__init__.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/compat.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/decoder.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/encoder.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/ordered_dict.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/scanner.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/tool.py")
EOF
python -O - <<'EOF' 
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/__init__.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/compat.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/decoder.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/encoder.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/ordered_dict.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/scanner.py")
py_compile.compile("build/lib.linux-x86_64-2.6/simplejson/tool.py")
EOF
cp build/lib.linux-x86_64-2.6/simplejson/*.pyo $TOPDIR/usr/lib64/python2.6/site-packages/simplejson
cp build/lib.linux-x86_64-2.6/simplejson/*.py $TOPDIR/usr/lib64/python2.6/site-packages/simplejson
cp build/lib.linux-x86_64-2.6/simplejson/*.pyc $TOPDIR/usr/lib64/python2.6/site-packages/simplejson
cp build/lib.linux-x86_64-2.6/simplejson/_speedups.so $TOPDIR/usr/lib64/python2.6/site-packages/simplejson
cp -r PKG-INFO $TOPDIR/usr/lib64/python2.6/site-packages/simplejson/simplejson.egg-info

cd $TOPDIR
#pyelasticsearch
cd opt/hltd/lib/pyelasticsearch-0.6/
./setup.py -q build
python - <<'EOF' 
import py_compile
py_compile.compile("build/lib/pyelasticsearch/__init__.py")
py_compile.compile("build/lib/pyelasticsearch/client.py")
py_compile.compile("build/lib/pyelasticsearch/downtime.py")
py_compile.compile("build/lib/pyelasticsearch/exceptions.py")
EOF
python -O - <<'EOF' 
import py_compile
py_compile.compile("build/lib/pyelasticsearch/__init__.py")
py_compile.compile("build/lib/pyelasticsearch/client.py")
py_compile.compile("build/lib/pyelasticsearch/downtime.py")
py_compile.compile("build/lib/pyelasticsearch/exceptions.py")
EOF
cp build/lib/pyelasticsearch/*.pyo $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch
cp build/lib/pyelasticsearch/*.py $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch
cp build/lib/pyelasticsearch/*.pyc $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch
cp -r pyelasticsearch.egg-info/ $TOPDIR/usr/lib64/python2.6/site-packages/pyelasticsearch

cd $TOPDIR
#python-prctl
cd opt/hltd/lib/python-prctl/
./setup.py -q build
python - <<'EOF' 
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/prctl.py")
EOF
python -O - <<'EOF' 
import py_compile
py_compile.compile("build/lib.linux-x86_64-2.6/prctl.py")
EOF
cp build/lib.linux-x86_64-2.6/prctl.pyo $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/prctl.py $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/prctl.pyc $TOPDIR/usr/lib64/python2.6/site-packages
cp build/lib.linux-x86_64-2.6/_prctl.so $TOPDIR/usr/lib64/python2.6/site-packages
cat > $TOPDIR/usr/lib64/python2.6/site-packages/python_prctl-1.5.0-py2.6.egg-info <<EOF
Metadata-Version: 1.0
Name: python-prctl
Version: 1.5.0
Summary: Python(ic) interface to the linux prctl syscall
Home-page: http://github.com/seveas/python-prctl
Author: Dennis Kaarsemaker
Author-email: dennis@kaarsemaker.net
License: UNKNOWN
Description: UNKNOWN
Platform: UNKNOWN
Classifier: Development Status :: 5 - Production/Stable
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: GNU General Public License (GPL)
Classifier: Operating System :: POSIX :: Linux
Classifier: Programming Language :: C
Classifier: Programming Language :: Python
Classifier: Topic :: Security
EOF

cd $TOPDIR
cd opt/hltd/lib/pyinotify-master/
./setup.py -q build
cp build/lib/pyinotify.py $TOPDIR/usr/lib64/python2.6/site-packages
python - <<'EOF' 
import py_compile
py_compile.compile("build/lib/pyinotify.py")
EOF
cp build/lib/pyinotify.pyc $TOPDIR/usr/lib64/python2.6/site-packages/
cat > $TOPDIR/usr/lib64/python2.6/site-packages/pyinotify-0.9.4-py2.6.egg-info <<EOF
Metadata-Version: 1.0
Name: pyinotify
Version: 0.9.4
Summary: Linux filesystem events monitoring
Home-page: http://github.com/seb-m/pyinotify
Author: Sebastien Martini
Author-email: seb@dbzteam.org
License: MIT License
Download-URL: http://pypi.python.org/pypi/pyinotify
Description: UNKNOWN
Platform: Linux
Classifier: Development Status :: 5 - Production/Stable
Classifier: Environment :: Console
Classifier: Intended Audience :: Developers
Classifier: License :: OSI Approved :: MIT License
Classifier: Natural Language :: English
Classifier: Operating System :: POSIX :: Linux
Classifier: Programming Language :: Python
Classifier: Programming Language :: Python :: 2.4
Classifier: Programming Language :: Python :: 2.5
Classifier: Programming Language :: Python :: 2.6
Classifier: Programming Language :: Python :: 2.7
Classifier: Programming Language :: Python :: 3
Classifier: Programming Language :: Python :: 3.0
Classifier: Programming Language :: Python :: 3.1
Classifier: Programming Language :: Python :: 3.2
Classifier: Topic :: Software Development :: Libraries :: Python Modules
Classifier: Topic :: System :: Filesystems
Classifier: Topic :: System :: Monitoring
EOF

cd $TOPDIR
# we are done here, write the specs and make the fu***** rpm
cat > hltd.spec <<EOF
Name: hltd
Version: 1.0
Release: 0
Summary: hlt daemon
License: gpl
Group: Hacks
Packager: zee-sub-wun
Source: none
%define _tmppath $TOPDIR/hltd-build
BuildRoot: %{_tmppath}
BuildArch: $BUILD_ARCH
AutoReqProv: no
Provides:/opt/hltd
Provides:/etc/hltd.conf
Provides:/etc/init.d/hltd
Provides:/usr/lib64/python2.6/site-packages/prctl.pyc
Provides:/usr/lib64/python2.6/site-packages/pyinotify.py
Requires:python,libcap,python-six,python-requests

%description
fff hlt daemon 

%prep
%build

%install
rm -rf \$RPM_BUILD_ROOT
mkdir -p \$RPM_BUILD_ROOT
tar -C $TOPDIR -c opt/hltd | tar -xC \$RPM_BUILD_ROOT
tar -C $TOPDIR -c etc | tar -xC \$RPM_BUILD_ROOT
tar -C $TOPDIR -c usr | tar -xC \$RPM_BUILD_ROOT
%post
rm -rf /etc/appliance/online/*
rm -rf /etc/appliance/offline/*
rm -rf /etc/appliance/except/*
/opt/hltd/python/fillresources.py
/sbin/service hltd restart
%files 
%defattr(-, root, root, -)
/opt/hltd/
/etc/hltd.conf
/etc/init.d/hltd
/etc/appliance
/usr/lib64/python2.6/site-packages/*prctl*
/usr/lib64/python2.6/site-packages/*simplejson*
/usr/lib64/python2.6/site-packages/*pyinotify*
/usr/lib64/python2.6/site-packages/pyelasticsearch
EOF
mkdir -p RPMBUILD/{RPMS/{noarch},SPECS,BUILD,SOURCES,SRPMS}
rpmbuild --define "_topdir `pwd`/RPMBUILD" -bb hltd.spec
#rm -rf patch-cmssw-tmp

