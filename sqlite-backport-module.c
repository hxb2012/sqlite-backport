/* Support for accessing SQLite databases.

Copyright (C) 2017 by Syohei YOSHIDA
Copyright (C) 2021-2022 Free Software Foundation, Inc.

This file is NOT part of GNU Emacs.

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at
your option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

This file is based on the emacs-sqlite3 package written by Syohei
YOSHIDA <syohex@gmail.com>, which can be found at:

https://github.com/syohex/emacs-sqlite3 */
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <emacs-module.h>
#include <sqlite3.h>

int plugin_is_GPL_compatible;

#define _SELECT(_1, _2, _3, _4, _5, N, ...) N
#define _COUNT(...) _SELECT(__VA_ARGS__, 5, 4, 3, 2, 1, 0)
#define Q(name) env->intern(env, #name)
#define build_string(s) env->make_string(env, (s), strlen((s)))
#define make_int(n) env->make_integer(env, n)
#define TYPE_OF(value) env->type_of(env, (value))
#define EQ(a, b) env->eq(env, (a), (b))
#define TYPEP(value, type) EQ(TYPE_OF(value), Q(type))
#define call(name, ...) env->funcall(env, Q(name), _COUNT(__VA_ARGS__), (emacs_value []){ __VA_ARGS__ })
#define xsignal(symbol, ...) env->non_local_exit_signal(env, Q(symbol), call(list, __VA_ARGS__))
#define NILP(value) !(env->is_not_nil(env, (value)))
#define XFIXNUM(value) env->extract_integer(env, (value))

struct Lisp_Sqlite {
  sqlite3 *db;
};

struct Lisp_Statement {
  sqlite3 *db;
  sqlite3_stmt *stmt;
  bool eof;
};

static
bool
CHECK_STRING(emacs_env *env, emacs_value value) {
  if (TYPEP(value, string))
    return true;
  xsignal(wrong-type-argument, Q(stringp), value);
  return false;
}

static
emacs_finalizer
user_ptr_check(emacs_env *env, emacs_value value) {
  if (TYPEP(value, user-ptr))
    return env->get_user_finalizer(env, value);
  return NULL;
}

static
void
lisp_sqlite_free(void *arg) {
  struct Lisp_Sqlite *ptr = (struct Lisp_Sqlite *)arg;
  if (ptr->db)
    sqlite3_close(ptr->db);
  free(ptr);
}

static
void
lisp_statement_free(void *arg) {
  struct Lisp_Statement *ptr = (struct Lisp_Statement *)arg;
  if (ptr->stmt)
    sqlite3_finalize(ptr->stmt);
  free(ptr);
}

static
emacs_value
lisp_sqlite_make(emacs_env *env, sqlite3 *db) {
  struct Lisp_Sqlite *ptr = malloc(sizeof(struct Lisp_Sqlite));
  ptr->db = db;
  return env->make_user_ptr(env, lisp_sqlite_free, ptr);
}

static
emacs_value
lisp_statement_make(emacs_env *env, sqlite3 *db, sqlite3_stmt *stmt) {
  struct Lisp_Statement *ptr = malloc(sizeof(struct Lisp_Statement));
  ptr->db = db;
  ptr->stmt = stmt;
  ptr->eof = false;
  return env->make_user_ptr(env, lisp_statement_free, ptr);
}

static
struct Lisp_Sqlite *
lisp_sqlite_check(emacs_env *env, emacs_value db) {
  emacs_finalizer finalizer = user_ptr_check(env, db);
  if (finalizer == lisp_sqlite_free) {
    struct Lisp_Sqlite *ptr = env->get_user_ptr(env, db);
    if (ptr->db)
      return ptr;
    xsignal(error, build_string("Database closed"));
  } else if (finalizer == lisp_statement_free) {
    xsignal(error, build_string("Invalid database object"));
  } else {
    xsignal(wrong-type-argument, Q(sqlitep), db);
  }
  return NULL;
}

static
struct Lisp_Statement *
lisp_statement_check(emacs_env *env, emacs_value stmt) {
  emacs_finalizer finalizer = user_ptr_check(env, stmt);
  if (finalizer == lisp_statement_free) {
    struct Lisp_Statement *ptr = env->get_user_ptr(env, stmt);
    if (ptr->stmt)
      return ptr;
    xsignal(error, build_string("Statement closed"));
  } else if (finalizer == lisp_sqlite_free) {
    xsignal(error, build_string("Invalid set object"));
  } else {
    xsignal(wrong-type-argument, Q(sqlitep), stmt);
  }
  return NULL;
}

static int db_count = 0;

static
emacs_value
Fsqlite_open(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  int flags = (SQLITE_OPEN_CREATE  | SQLITE_OPEN_READWRITE);
#ifdef SQLITE_OPEN_FULLMUTEX
  flags |= SQLITE_OPEN_FULLMUTEX;
#endif
#ifdef SQLITE_OPEN_URI
  flags |= SQLITE_OPEN_URI;
#endif

  emacs_value name;
  if ((nargs > 0) && !NILP(args[0])) {
    name = call(expand_file_name, args[0], Q(nil));
  } else {
#ifdef SQLITE_OPEN_MEMORY
    /* In-memory database.  These have to have different names to
       refer to different databases.  */
    name = call(format, build_string(":memory:%d"), make_int(++db_count));
    flags |= SQLITE_OPEN_MEMORY;
#else
    xsignal(error, build_string("sqlite in-memory is not available"));
#endif
  }

  ptrdiff_t size=0;
  env->copy_string_contents(env, name, NULL, &size);
  char *encoded = malloc(size);
  env->copy_string_contents(env, name, encoded, &size);

  sqlite3 *sdb;
  int ret = sqlite3_open_v2 (encoded, &sdb, flags, NULL);
  free(encoded);
  if (ret != SQLITE_OK)
    return Q(nil);

  return lisp_sqlite_make(env, sdb);
}

static
emacs_value
Fsqlite_close(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  sqlite3_close(ptr->db);
  ptr->db = NULL;
  return Q(t);
}

/* Bind values in a statement like
   "insert into foo values (?, ?, ?)".  */
static
const char *
bind_values (emacs_env *env, sqlite3 *db, sqlite3_stmt *stmt, emacs_value values) {
  sqlite3_reset(stmt);
  bool is_vector = TYPEP(values, vector);
  int len = (is_vector)?env->vec_size(env, values):XFIXNUM(call(length, values));

  for (int i = 0; i < len; ++i) {
    int ret = SQLITE_MISMATCH;
    emacs_value value;
    if (is_vector) {
      value = env->vec_get(env, values, i);
    } else {
      value = call(car, values);
      values = call(cdr, values);
    }

    emacs_value type = TYPE_OF(value);
    if (EQ(type, Q(string))) {
      bool blob = false;

      emacs_value coding_system = call(get-text-property, make_int(0), Q(coding-system), value);
      if (!NILP(coding_system)) {
        if (EQ(coding_system, Q(binary))) {
            blob = true;
        } else {
          value = call(encode-coding-string, value, coding_system, Q(nil), Q(nil));
        }
      }

      ptrdiff_t size = 0;
      env->copy_string_contents(env, value, NULL, &size);

      if (blob) {
        if (size) {
          if (size != XFIXNUM(call(length, value)) + 1) {
            xsignal(error, build_string("BLOB values must be unibyte"));
            return "";
          }

          char *encoded = malloc(size);
          env->copy_string_contents(env, value, encoded, &size);
          ret = sqlite3_bind_blob(stmt, i + 1, encoded, size - 1, free);
        } else {
          ret = sqlite3_bind_blob(stmt, i + 1, NULL, 0, NULL);
        }
      } else {
        if (size) {
          char *encoded = malloc(size);
          env->copy_string_contents(env, value, encoded, &size);
          ret = sqlite3_bind_text(stmt, i + 1, encoded, size - 1, free);
        } else {
          ret = sqlite3_bind_text(stmt, i + 1, NULL, 0, NULL);
        }
      }
    } else if (EQ(type, Q(integer))) {
      ret = sqlite3_bind_int64(stmt, i + 1, XFIXNUM(value));
    } else if (EQ(type, Q(float))) {
      ret = sqlite3_bind_double(stmt, i + 1, env->extract_float(env, value));
    } else if (NILP(value)) {
        ret = sqlite3_bind_null(stmt, i + 1);
    } else if (EQ(value, Q(t))) {
      ret = sqlite3_bind_int(stmt, i + 1, 1);
    } else if (EQ(value, Q(false))) {
      ret = sqlite3_bind_int(stmt, i + 1, 0);
    } else {
      return "invalid argument";
    }

    if (ret != SQLITE_OK)
      return sqlite3_errmsg (db);
  }

  return NULL;
}

static
emacs_value
Fsqlite_execute(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  if(!CHECK_STRING(env, args[1]))
    return Q(nil);

  if (nargs > 2) {
    emacs_value type = TYPE_OF(args[2]);
    if (!(NILP(args[2]) || EQ(type, Q(cons)) || EQ(type, Q(vector)))) {
      xsignal(error, build_string ("VALUES must be a list or a vector"));
      return Q(nil);
    }
  }

  const char *errmsg = NULL;
  ptrdiff_t size = 0;
  env->copy_string_contents(env, args[1], NULL, &size);
  char *encoded = malloc(size);
  env->copy_string_contents(env, args[1], encoded, &size);
  sqlite3_stmt *stmt = NULL;

  /* We only execute the first statement -- if there's several
     (separated by a semicolon), the subsequent statements won't be
     done.  */
  int ret = sqlite3_prepare_v2(ptr->db, encoded, -1, &stmt, NULL);
  free(encoded);

  if (ret != SQLITE_OK) {
    if (stmt) {
      sqlite3_finalize (stmt);
      sqlite3_reset (stmt);
    }

    errmsg = sqlite3_errmsg(ptr->db);
    goto exit;
  }

  /* Bind ? values.  */
  if ((nargs > 2) && !NILP (args[2])) {
    const char *err = bind_values(env, ptr->db, stmt, args[2]);
    if (err) {
      errmsg = err;
      goto exit;
    }
  }

  ret = sqlite3_step(stmt);
  sqlite3_finalize(stmt);
  if (ret != SQLITE_OK && ret != SQLITE_DONE) {
    errmsg = sqlite3_errmsg(ptr->db);
    goto exit;
  }

  return make_int(sqlite3_changes(ptr->db));
 exit:
  if (ret == SQLITE_LOCKED || ret == SQLITE_BUSY) {
    xsignal(sqlite_locked_error, build_string(errmsg));
  } else {
    xsignal(error, build_string(errmsg));
  }

  return Q(nil);
}

static
emacs_value
row_to_value(emacs_env *env, sqlite3_stmt *stmt) {
  int len = sqlite3_column_count(stmt);
  emacs_value values = Q(nil);

  for (int i = 0; i < len; ++i) {
    emacs_value v;

    switch (sqlite3_column_type (stmt, i)) {
    case SQLITE_INTEGER:
      v = make_int(sqlite3_column_int64(stmt, i));
      break;
    case SQLITE_FLOAT:
      v = env->make_float(env, sqlite3_column_double(stmt, i));
      break;
    case SQLITE_BLOB:
      v = env->make_unibyte_string(env,
                                   sqlite3_column_blob(stmt, i),
                                   sqlite3_column_bytes(stmt, i));
      break;
    case SQLITE_TEXT:
      v = env->make_string(env,
                           (const char *)sqlite3_column_text(stmt, i),
                           sqlite3_column_bytes(stmt, i));
      break;
    default:
      v = Q(nil);
      break;
    }

    values = call(cons, v, values);
  }

  return call(nreverse, values);
}

static
emacs_value
column_names(emacs_env *env, sqlite3_stmt *stmt) {
  emacs_value columns = Q(nil);
  int count = sqlite3_column_count(stmt);
  for (int i = 0; i < count; ++i)
    columns = call(cons, build_string(sqlite3_column_name(stmt, i)), columns);

  return call(nreverse, columns);
}

static
emacs_value
Fsqlite_select(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  if(!CHECK_STRING(env, args[1]))
    return Q(nil);

  const char *errmsg = NULL;
  ptrdiff_t size = 0;
  env->copy_string_contents(env, args[1], NULL, &size);
  char *encoded = malloc(size);
  env->copy_string_contents(env, args[1], encoded, &size);
  sqlite3_stmt *stmt = NULL;

  int ret = sqlite3_prepare_v2(ptr->db, encoded, size, &stmt, NULL);
  free(encoded);

  if (ret != SQLITE_OK) {
    if (stmt)
      sqlite3_finalize (stmt);
    errmsg = sqlite_prepare_errdata (ret, sdb);
    goto exit;
  }

  /* Query with parameters.  */
  if ((nargs > 2) && !NILP(args[2])) {
    const char *err = bind_values(env, ptr->db, stmt, args[2]);
    if (err) {
      sqlite3_finalize (stmt);
      errmsg = err;
      goto exit;
    }
  }

  /* Return a handle to get the data.  */
  if ((nargs > 3) && EQ(args[3], Q(set)))
    return lisp_statement_make(env, ptr->db, stmt);

  /* Return the data directly.  */
  emacs_value retval = Q(nil);

  while ((ret = sqlite3_step(stmt)) == SQLITE_ROW)
    retval = call(cons, row_to_value(env, stmt), retval);

  retval = call(nreverse, retval);

  if ((nargs > 3) && EQ(args[3], Q(full)))
    retval = call(cons, column_names(env, stmt), retval);

  sqlite3_finalize (stmt);
  return retval;
 exit:
  xsignal(error, build_string(errmsg));
  return Q(nil);
}

static
emacs_value
Fsqlite_transaction(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  int ret = sqlite3_exec(ptr->db, "begin", NULL, NULL, NULL);
  if (ret != SQLITE_OK)
    return Q(nil);
  return Q(t);
}

static
emacs_value
Fsqlite_commit(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  int ret = sqlite3_exec(ptr->db, "commit", NULL, NULL, NULL);
  if (ret != SQLITE_OK)
    return Q(nil);
  return Q(t);
}

static
emacs_value
Fsqlite_rollback(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  int ret = sqlite3_exec(ptr->db, "rollback", NULL, NULL, NULL);
  if (ret != SQLITE_OK)
    return Q(nil);
  return Q(t);
}

static
emacs_value
Fsqlite_pragma(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Sqlite *ptr = lisp_sqlite_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  emacs_value pragma = call(concat, build_string("PRAGMA "), args[1]);

  ptrdiff_t size = 0;
  env->copy_string_contents(env, pragma, NULL, &size);
  char *encoded = malloc(size);
  env->copy_string_contents(env, pragma, encoded, &size);

  int ret = sqlite3_exec(ptr->db, encoded, NULL, NULL, NULL);
  free(encoded);
  if (ret != SQLITE_OK)
    return Q(nil);
  return Q(t);
}

static
emacs_value
Fsqlite_next(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Statement *ptr = lisp_statement_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  int ret = sqlite3_step(ptr->stmt);
  if (ret != SQLITE_ROW && ret != SQLITE_OK && ret != SQLITE_DONE) {
    xsignal(error, build_string(sqlite3_errmsg(ptr->db)));
    return Q(nil);
  }

  if (ret == SQLITE_DONE) {
    ptr->eof = true;
    return Q(nil);
  }

  return row_to_value(env, ptr->stmt);
}

static
emacs_value
Fsqlite_columns(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Statement *ptr = lisp_statement_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  return column_names(env, ptr->stmt);
}

static
emacs_value
Fsqlite_more_p(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Statement *ptr = lisp_statement_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  if (!ptr->eof)
    return Q(t);
  return Q(nil);
}

static
emacs_value
Fsqlite_finalize(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  struct Lisp_Statement *ptr = lisp_statement_check(env, args[0]);
  if (!ptr)
    return Q(nil);

  sqlite3_finalize(ptr->stmt);
  ptr->stmt = NULL;
  return Q(t);
}

static
emacs_value
Fsqlitep(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs], void *data __attribute__((unused))) {
  emacs_finalizer finalizer = user_ptr_check(env, args[0]);
  if (finalizer == lisp_sqlite_free)
    return Q(t);
  if (finalizer == lisp_statement_free)
    return Q(t);
  return Q(nil);
}

static
emacs_value
Fsqlite_available_p(emacs_env *env, ptrdiff_t nargs, emacs_value args[nargs] __attribute__((unused)), void *data __attribute__((unused))) {
  return Q(t);
}

int
emacs_module_init(struct emacs_runtime *ert) {
  emacs_env *env = ert->get_environment(ert);

  struct {
    const char *name;
    ptrdiff_t min_arity;
    ptrdiff_t max_arity;
    emacs_value (*func) (emacs_env *env,
                         ptrdiff_t nargs,
                         emacs_value* args,
                         void *data)
      EMACS_NOEXCEPT
      EMACS_ATTRIBUTE_NONNULL(1);
    const char *docstring;
  } funcs[] = {
    {"sqlite-open", 0, 1, Fsqlite_open,
     "Open FILE as an sqlite database.\n"
     "If FILE is nil, an in-memory database will be opened instead."},
    {"sqlite-close", 1, 1, Fsqlite_close,
     "Close the sqlite database DB."},
    {"sqlite-execute", 2, 3, Fsqlite_execute,
     "If VALUES is non-nil, it should be a vector or a list of values\n"
     "to bind when executing a statement like\n"
     "\n"
     "   insert into foo values (?, ?, ...)\n"
     "\n"
     "Value is the number of affected rows."},
    {"sqlite-select", 2, 4, Fsqlite_select,
     "Select data from the database DB that matches QUERY.\n"
     "If VALUES is non-nil, it should be a list or a vector specifying the\n"
     "values that will be interpolated into a parameterized statement.\n"
     "\n"
     "By default, the return value is a list where the first element is a\n"
     "list of column names, and the rest of the elements are the matching data.\n"
     "\n"
     "RETURN-TYPE can be either nil (which means that the matching data\n"
     "should be returned as a list of rows), or `full' (the same, but the\n"
     "first element in the return list will be the column names), or `set',\n"
     "which means that we return a set object that can be queried with\n"
     "`sqlite-next' and other functions to get the data."},
    {"sqlite-transaction", 1, 1, Fsqlite_transaction,
     "Start a transaction in DB."},
    {"sqlite-commit", 1, 1, Fsqlite_commit,
     "Commit a transaction in DB."},
    {"sqlite-rollback", 1, 1, Fsqlite_rollback,
     "Roll back a transaction in DB."},
    {"sqlite-pragma", 2, 2, Fsqlite_pragma,
     "Execute PRAGMA in DB."},
    {"sqlite-next", 1, 1, Fsqlite_next,
     "Return the next result set from SET."},
    {"sqlite-columns", 1, 1, Fsqlite_columns,
     "Return the column names of SET."},
    {"sqlite-more-p", 1, 1, Fsqlite_more_p,
     "Say whether there are any further results in SET."},
    {"sqlite-finalize", 1, 1, Fsqlite_finalize,
     "Mark this SET as being finished.\n"
     "This will free the resources held by SET."},
    {"sqlitep", 1, 1, Fsqlitep,
     "Say whether OBJECT is an SQlite object."},
    {"sqlite-available-p", 0, 0, Fsqlite_available_p,
     "Return t if sqlite3 support is available in this instance of Emacs."}
  };

  for(size_t i=0; i<sizeof(funcs)/sizeof(funcs[0]); i++) {
    emacs_value sym = env->intern(env, funcs[i].name);
    emacs_value fun =
      env->make_function(env,
                         funcs[i].min_arity,
                         funcs[i].max_arity,
                         funcs[i].func,
                         funcs[i].docstring,
                         NULL);
    call(fset, sym, fun);
  }

  call(provide, Q(sqlite-backport-module));
  return 0;
}
