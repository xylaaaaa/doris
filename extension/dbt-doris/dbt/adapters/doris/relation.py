#!/usr/bin/env python
# encoding: utf-8

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy


@dataclass
class DorisQuotePolicy(Policy):
    # Doris uses the dbt relation path as catalog.database.table.  For an
    # internal-catalog relation ``database`` is None, so enabling this part
    # keeps the normal two-part ``database.table`` rendering unchanged.
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class DorisIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class DorisRelation(BaseRelation):
    quote_policy: DorisQuotePolicy = field(default_factory=lambda: DorisQuotePolicy())
    include_policy: DorisIncludePolicy = field(default_factory=lambda: DorisIncludePolicy())
    quote_character: str = "`"

    @classmethod
    def create_from(cls, quoting, relation_config, **kwargs):
        relation = super().create_from(quoting, relation_config, **kwargs)

        # dbt's snapshot API historically called both target_database and
        # target_schema with the same Doris Database name.  Preserve that
        # public form as an internal-catalog two-part relation while allowing
        # different values to represent catalog.database.table.
        if str(getattr(relation_config, "resource_type", "")) == "snapshot":
            snapshot_config = getattr(relation_config, "config", None)
            target_database = getattr(snapshot_config, "target_database", None)
            target_schema = getattr(snapshot_config, "target_schema", None)
            if target_database and target_database == target_schema:
                relation = relation.replace_path(
                    database=None,
                    schema=target_schema,
                )

        return relation

    def __post_init__(self):
        # dbt may use an empty string, or the legacy stringified ``None``, for
        # an omitted database in a model relation. Treat both as None so they
        # do not render an empty quoted identifier.
        if self.database in ("", "None"):
            self.path.database = None

    def quoted(self, identifier):
        return "{}{}{}".format(
            self.quote_character,
            str(identifier).replace(self.quote_character, self.quote_character * 2),
            self.quote_character,
        )
