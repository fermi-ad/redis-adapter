BI_PREFIX  = $(prefix)
BI_PROJECT = redisAdapter
BI_VERSION = 0
BI_MINOR   = 0
BI_RELEASE = 1
REDISADAPTER_VERSION = $(BI_VERSION).$(BI_MINOR).$(BI_RELEASE)

# Install locations
BI_INC = /usr/local/include
BI_LIB += /usr/local/lib
BI_PRODUCTS_INC = /usr/local/products/include
BI_PRODUCTS_LIB += /usr/local/products/lib


BI_OUT =

# specify all header files to be installed in the includes directory
HEADERS      = $(wildcard [!_]*.hpp)

BI_CPPFLAGS = 	-I. \
				-I$(BI_INC) \
				-DLINUX \
				-g \
				-Wall \
				-pipe \
				-O2 \
				-fPIC \
				-Wno-format-extra-args \
				-Wno-literal-suffix \
				-std=c++2a \
				-D_REENTRANT \
				-DVERSION_ID=$(REDISADAPTER_VERSION)

BI_LDFLAGS  = 	-L/usr/local/lib/ \
				-L/usr/local/lib64/ \
				-L. \
				-lredis++ \
				-lhiredis \
				-pthread \
				-lrt


LIB_STATIC   = lib$(BI_PROJECT).a
SO_NAME      = lib$(BI_PROJECT).so
SO_VER_NAME  = lib$(BI_PROJECT).so.$(BI_VERSION)
SO_LONG_NAME = lib$(BI_PROJECT).so.$(BI_VERSION).$(BI_MINOR).$(BI_RELEASE)

# List of modules (.o files)
BI_CPPSOURCES_A = $(wildcard *.cpp)
BI_OBJS = $(BI_CPPSOURCES_A:.cpp=.o)

all: clean libredisadapter

%.o: %.cpp Makefile
	$(BI_OUT) $(CXX) -c -o $*.o $(BI_CPPFLAGS) $<

test: test.o
	$(CXX) ./test.o -o test $(BI_LDFLAGS) -lredisAdapter

test2: test2.o
	$(CXX) ./test2.o -o test2 $(BI_LDFLAGS) -lredisAdapter

libredisadapter: RedisAdapter.o
	$(BI_OUT) $(CXX) -shared -Wl,-soname,$(SO_NAME) -o $(SO_LONG_NAME)  RedisAdapter.o -lm $(BI_LDFLAGS)
	$(AR) rcs libredisAdapter.a RedisAdapter.o

clean:
	rm -f *.o *.a *.so* test test2

install:
	install -d $(BI_LIB)
	install -D -m 0755 $(LIB_STATIC) $(BI_LIB)
	install -D -m 0755 $(SO_LONG_NAME) $(BI_LIB)
	ln -sf $(SO_LONG_NAME) $(BI_LIB)/$(SO_VER_NAME)
	ln -sf $(SO_VER_NAME) $(BI_LIB)/$(SO_NAME)
	install -d $(BI_INC)
	install -D -m 0644  $(HEADERS) $(BI_INC)

installp:
	install -d $(BI_PRODUCTS_LIB)
	install -D -m 0755 $(LIB_STATIC ) $(BI_PRODUTCS_LIB)
	install -D -m 0755 $(SO_LONG_NAME) $(BI_PRODUCTS_LIB)
	ln -sf $(SO_LONG_NAME) $(BI_PRODUCTS_LIB)/$(SO_VER_NAME)
	ln -sf $(SO_VER_NAME) $(BI_PRODUCTSLIB)/$(SO_NAME0)
	install -d $(BI_PRODUCTS_INC)
	install -D -m 0644  $(HEADERS) $(BI_PRODUCTS_INC)


.PHONY: clean




