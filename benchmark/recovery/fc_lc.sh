detectLC() {
  for x in {1..25}; do pushd $x > /dev/null; grep 'Suspecting [^0]' * > /dev/null && echo $x; popd > /dev/null; done
}

detectFC() {
  for x in {1..25}; do pushd $x > /dev/null; grep 'Suspecting [^0]' * > /dev/null || echo $x; popd > /dev/null; done
}

pushd ${1?"Missing argument 
                   ?
                  / \\
                 /   \\
                / ^ ^ \\
               (| O.0 |)
                | -=- |
                 \___/"}

for x in $(find -maxdepth 1 -mindepth 1 -type d)
do
  pushd $x 

  mkdir -p lc
  mkdir -p fc

  for f in *
  do
    if [[ "$f" =~ ^[[:digit:]]*$ ]]
    then
      if grep 'Suspecting 1 on view 1' $f/* 
      then
        echo mv $f lc && mv $f lc
      else
        echo mv $f fc && mv $f fc
      fi
    fi
  done

  popd
done
  
popd