/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive.security;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class LegacySecurityConfig
{
    private boolean allowAddColumn = true;
    private boolean allowDropColumn = true;
    private boolean allowDropTable = true;
    private boolean allowRenameTable = true;
    private boolean allowRenameColumn = true;
    private boolean allowDropConstraint = true;
    private boolean allowAddConstraint = true;

    public boolean getAllowAddColumn()
    {
        return this.allowAddColumn;
    }

    @Config("hive.allow-add-column")
    @ConfigDescription("Allow Hive connector to add column")
    public LegacySecurityConfig setAllowAddColumn(boolean allowAddColumn)
    {
        this.allowAddColumn = allowAddColumn;
        return this;
    }

    public boolean getAllowDropColumn()
    {
        return this.allowDropColumn;
    }

    @Config("hive.allow-drop-column")
    @ConfigDescription("Allow Hive connector to drop column")
    public LegacySecurityConfig setAllowDropColumn(boolean allowDropColumn)
    {
        this.allowDropColumn = allowDropColumn;
        return this;
    }

    public boolean getAllowDropTable()
    {
        return this.allowDropTable;
    }

    @Config("hive.allow-drop-table")
    @ConfigDescription("Allow Hive connector to drop table")
    public LegacySecurityConfig setAllowDropTable(boolean allowDropTable)
    {
        this.allowDropTable = allowDropTable;
        return this;
    }

    public boolean getAllowRenameTable()
    {
        return this.allowRenameTable;
    }

    @Config("hive.allow-rename-table")
    @ConfigDescription("Allow Hive connector to rename table")
    public LegacySecurityConfig setAllowRenameTable(boolean allowRenameTable)
    {
        this.allowRenameTable = allowRenameTable;
        return this;
    }

    public boolean getAllowRenameColumn()
    {
        return this.allowRenameColumn;
    }

    @Config("hive.allow-rename-column")
    @ConfigDescription("Allow Hive connector to rename column")
    public LegacySecurityConfig setAllowRenameColumn(boolean allowRenameColumn)
    {
        this.allowRenameColumn = allowRenameColumn;
        return this;
    }

    public boolean getAllowDropConstraint()
    {
        return this.allowDropConstraint;
    }

    @Config("hive.allow-drop-constraint")
    @ConfigDescription("Allow Hive connector to drop constraint")
    public LegacySecurityConfig setAllowDropConstraint(boolean allowDropConstraint)
    {
        this.allowDropConstraint = allowDropConstraint;
        return this;
    }

    public boolean getAllowAddConstraint()
    {
        return this.allowAddConstraint;
    }

    @Config("hive.allow-add-constraint")
    @ConfigDescription("Allow Hive connector to add constraints")
    public LegacySecurityConfig setAllowAddConstraint(boolean allowAddConstraint)
    {
        this.allowAddConstraint = allowAddConstraint;
        return this;
    }
}
