#ifndef POSTGRAPH_H
#define POSTGRAPH_H

#include "postgres.h"

#include "fmgr.h"

#include "port.h"

extern PGDLLIMPORT pid_t GraphmasterPid;
extern PGDLLIMPORT bool IsGraphmasterEnvironment;
extern PGDLLIMPORT bool IsUnderGraphmaster;
extern PGDLLIMPORT bool IsGraphmasterEnvironment;
extern PGDLLIMPORT MemoryContext GraphmasterContext;


void
InitGraphmasterChild(void);

void
PostGraphMain(int argc, char *argv[],
                         const char *dbname,
                         const char *username);

void
InitPostGraph(const char *in_dbname, Oid dboid, const char *username,
                         Oid useroid, char *out_dbname, bool override_allow_connections);

void
graphmaster_start_new(Datum arg);

void
PostGraphInitStandaloneProcess(const char *argv0);

void
graphmaster_start(Datum arg);

int
PostGraphStreamServerPort(int family, const char *hostName, unsigned short portNumber,
                                 const char *unixSocketDir,
                                 pgsocket ListenSocket[], int MaxListen);

void
PostGraphCreateSocketLockFile(const char *socketfile, bool amPostmaster,
                                         const char *socketDir);
void
PostGraphAuxiliaryProcessMain(int argc, char *argv[]);


void
PostGraphRemoveSocketFiles(void);

#endif
