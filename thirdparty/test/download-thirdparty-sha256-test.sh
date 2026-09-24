#!/usr/bin/env bash
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

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

stub_bin="${tmpdir}/bin"
mkdir -p "${stub_bin}"
payload="paimon ORC archive payload"
payload_sha256="$(printf '%s' "${payload}" | sha256sum | awk '{print $1}')"

cat > "${tmpdir}/vars.sh" <<EOF
#!/bin/bash
export TP_SOURCE_DIR="${tmpdir}/src"
export TP_INSTALL_DIR="${tmpdir}/installed"
export TP_PATCH_DIR="${tmpdir}/patches"
export TP_INCLUDE_DIR="\${TP_INSTALL_DIR}/include"
export TP_LIB_DIR="\${TP_INSTALL_DIR}/lib"
export TP_JAR_DIR="\${TP_INSTALL_DIR}/lib/jar"
FOO_DOWNLOAD="https://official.example.com/foo.tar.gz"
FOO_NAME="foo.tar.gz"
FOO_SOURCE=
FOO_SHA256SUM="${payload_sha256}"
export TP_ARCHIVES=('FOO')
EOF

cat > "${stub_bin}/wget" <<EOF
#!/usr/bin/env bash
set -eo pipefail
url=""
output=""
expect_output=0
for arg in "\$@"; do
    if [[ "\${expect_output}" -eq 1 ]]; then
        output="\${arg}"
        expect_output=0
        continue
    fi
    if [[ "\${arg}" == "-O" ]]; then
        expect_output=1
        continue
    fi
    if [[ "\${arg}" != -* && -z "\${url}" ]]; then
        url="\${arg}"
    fi
done
if [[ "\${url}" == "https://repo.example.com/thirdparty/foo.tar.gz" ]]; then
    printf '%s' "${payload}" > "\${output}"
    exit 0
fi
exit 1
EOF
chmod +x "${stub_bin}/wget"

PATH="${stub_bin}:${PATH}" REPOSITORY_URL="https://repo.example.com/thirdparty" \
    TP_DIR="${tmpdir}" DORIS_HOME="${ROOT}/.." \
    bash "${ROOT}/download-thirdparty.sh"

[[ "$(cat "${tmpdir}/src/foo.tar.gz")" == "${payload}" ]]
echo "PASS"
