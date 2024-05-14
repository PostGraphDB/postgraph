/*
 * PostGraph
 * Copyright (C) 2023 PostGraphDB
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "postgres.h"

#include "fmgr.h"

#include "postmaster/bgworker.h"
#include "miscadmin.h"
PG_MODULE_MAGIC;

void _PG_init(void);

void _PG_init(void) {
        if (!process_shared_preload_libraries_in_progress)
                return;


	BackgroundWorkerHandle *bgw_handle;
	BackgroundWorker bgw = {.bgw_name = "graphmaster",
		.bgw_type = "graphmaster",
		.bgw_library_name = "postgraph",
		.bgw_function_name = "graphmaster_start_new",
		.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION,
		.bgw_start_time = BgWorkerStart_RecoveryFinished};
        bgw.bgw_restart_time = BGW_NEVER_RESTART;
        bgw.bgw_notify_pid = 0;
	bgw.bgw_main_arg = ObjectIdGetDatum(1);
	RegisterBackgroundWorker(&bgw);
	//RegisterDynamicBackgroundWorker(&bgw, NULL);
}

void _PG_fini(void);

void _PG_fini(void) {
}
