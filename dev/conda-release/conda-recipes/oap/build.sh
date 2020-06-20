mkdir -p $PREFIX/jars
cp $RECIPE_DIR/*.jar $PREFIX/jars/
mkdir -p $PREFIX/lib64
cp $RECIPE_DIR/libmemkind.so.0 $PREFIX/lib64/libmemkind.so.0
