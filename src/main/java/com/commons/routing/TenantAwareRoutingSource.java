package com.commons.routing;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;
import com.commons.core.ThreadLocalStorage;

public class TenantAwareRoutingSource extends AbstractRoutingDataSource {
	
    @Override
    protected Object determineCurrentLookupKey() {
        return ThreadLocalStorage.getTenantName();
    }

}
