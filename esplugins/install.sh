cd $1
echo installing elasticsearch plugins...
bin/plugin --url file:///opt/fff/esplugins/$2 --install $3

