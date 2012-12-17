package com.rokhmanov.sample.teiid.cache;

import java.util.Date;
import java.util.List;

import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.CacheDirective.Scope;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.google.common.collect.Lists;

public class TestCacheExecution implements ResultSetExecution {

    private int numRowsToCreate = 2;
    private ExecutionContext context;
    private QueryExpression command;
        
	public TestCacheExecution(ExecutionContext context, QueryExpression command) {
		this.context = context;
		this.command = command;
	}

	// Disable cache if SELECT statement has WHERE... condition
	public void execute() throws TranslatorException {
		CacheDirective cd = context.getCacheDirective();
		if (null != ((Select)command).getWhere())
		{
			cd.setScope(Scope.NONE);			
		} else 
		{
			cd.setScope(Scope.USER);
		}
	}

	public List<?> next() throws TranslatorException, DataNotAvailableException 
	{
        if (this.numRowsToCreate == 0)
        {
            return null;
        }
        try
        {
            return Lists.newArrayList(numRowsToCreate, new Date());
        } finally
        {
            this.numRowsToCreate--;
        }
	}	
	

	public void close() {
		// TODO Auto-generated method stub

	}

	public void cancel() throws TranslatorException {
		// TODO Auto-generated method stub

	}
}
