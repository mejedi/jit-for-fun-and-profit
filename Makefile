schema_util.so: schema_util.c
	gcc -O3 -DNDEBUG -std=c99 -fno-strict-aliasing -fPIC -shared schema_util.c -o schema_util.so

