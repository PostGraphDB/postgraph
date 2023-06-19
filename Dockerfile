# Copyright (C) 2023 PostGraphDB
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
# 
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# 
# For PostgreSQL Database Management System:
# (formerly known as Postgres, then as Postgres95)
# 
# Portions Copyright (c) 2020-2023, Apache Software Foundation

FROM postgres:14

RUN apt-get update
RUN apt-get install --assume-yes --no-install-recommends --no-install-suggests \
  bison \
  build-essential \
  flex \
  postgresql-server-dev-14

COPY . /postgraph
RUN cd /postgraph && make install

COPY docker-entrypoint-initdb.d/00-create-extension-postgraph.sql /docker-entrypoint-initdb.d/00-create-extension-postgraph.sql

CMD ["postgres", "-c", "shared_preload_libraries=postgraph"]
