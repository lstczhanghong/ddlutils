package org.apache.ddlutils.platform.dm;

import org.apache.ddlutils.platform.PlatformImplBase;
/**
 * The platform implementation for dm db.
 *
 * @version $Revision: 231306 $
 */
public class DmPlatform extends PlatformImplBase {
    /**
     * Database name of this platform.
     */
    public static final String DATABASENAME = "DM";
    /**
     * The standard dm jdbc driver.
     */
    public static final String JDBC_DRIVER = "dm.jdbc.driver.DmDriver";

    public String getName() {
        return null;
    }
}
