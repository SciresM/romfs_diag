/*
 * Copyright (c) Atmosphère-NX
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms and conditions of the GNU General Public License,
 * version 2, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
 * more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#include <stratosphere.hpp>
#include "romfs_diag_romfs.hpp"

namespace ams {

    void Main() {
        /* Disable auto-abort when performing file operations. */
        fs::SetEnabledAutoAbort(false);

        const auto argc = os::GetHostArgc();
        const auto argv = os::GetHostArgv();

        if (argc != 2) {
            printf("Usage: %s dir", argv[0]);
            return;
        }

        // TODO: the thing
        printf("Building romfs for %s\n", argv[1]);
        romfs_diag::Builder builder;
        builder.AddSdFiles(argv[1]);

        romfs_diag::Builder::SourceInfoVector infos;
        builder.Build(std::addressof(infos));
        printf("Done\n");
    }

}