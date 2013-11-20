##############################################################################
#
# WhiteDB Foreign Data Wrapper for PostgreSQL
#
# Copyright (c) 2013 Ian Pye
#
# This software is released under the PostgreSQL Licence
#
# Author: Ian Pye <ian@chimera.io>
#
# IDENTIFICATION
#        wdb_fdw/Makefile
#
##############################################################################


EXTENSION    = wdb_fdw
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
DOCS         = $(wildcard doc/*.md)
USE_MODULE_DB = 1
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --outputdir=test \
	--load-language=plpgsql --load-extension=$(EXTENSION)
MODULE_big      = $(EXTENSION)
OBJS         =  $(patsubst %.c,%.o,$(wildcard src/*.c))
PG_CONFIG    = /home/ian/local/bin/pg_config
SHLIB_LINK = -lwgdb -lsasl2

CXXFLAGS = -I/home/ian/local/include -march=native -m64 -g -O0 -Wall -fPIC -fsigned-char -Wno-unused-but-set-variable -Wno-unused-but-set-parameter
PG_CPPFLAGS = --std=c99 -DUSE_TRANSACTIONS

all: sql/$(EXTENSION)--$(EXTVERSION).sql

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@

DATA_built = sql/$(EXTENSION)--$(EXTVERSION).sql
DATA = $(filter-out sql/$(EXTENSION)--$(EXTVERSION).sql, $(wildcard sql/*--*.sql))
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# we put all the tests in a test subdir, but pgxs expects us not to, darn it
override pg_regress_clean_files = test/results/ test/regression.diffs test/regression.out tmp_check/ log/
