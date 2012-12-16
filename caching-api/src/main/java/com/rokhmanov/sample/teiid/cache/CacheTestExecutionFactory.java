package com.rokhmanov.sample.teiid.cache;

import java.util.List;

import org.teiid.language.Command;
import org.teiid.language.NamedTable;
import org.teiid.language.QueryExpression;
import org.teiid.language.TableReference;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.CacheDirective;
import org.teiid.translator.CacheDirective.Scope;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@Translator(name = "test-caching")
public class CacheTestExecutionFactory  extends ExecutionFactory<Object, Object> {

	public CacheTestExecutionFactory() {
		super();
		this.setSourceRequired(false);
	}
	
	@Override
	public CacheDirective getCacheDirective(Command command,
			ExecutionContext executionContext, RuntimeMetadata metadata)
			throws TranslatorException {
		CacheDirective cd =  new CacheDirective();
		cd.setScope(Scope.USER);
		return cd;
	}

	
	@Override
	public ResultSetExecution createResultSetExecution(QueryExpression command,
			ExecutionContext executionContext, RuntimeMetadata metadata,
			Object connection) throws TranslatorException 
	{
        final List<TableReference> froms = command.getProjectedQuery().getFrom();
        if (froms.get(0) instanceof NamedTable)
        {
            final NamedTable table = (NamedTable) froms.get(0);
            if (table.getName().equalsIgnoreCase("test"))
            {
        		return new TestCacheExecution(executionContext, command);
            }
        }
        throw new TranslatorException("Unknown table name:" + froms.get(0));
	}

	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, Object conn)
			throws TranslatorException 
	{        
        final Table testTable = metadataFactory.addTable("test");
        metadataFactory.addColumn("id", TypeFacility.RUNTIME_NAMES.INTEGER, testTable);
        metadataFactory.addColumn("timer", TypeFacility.RUNTIME_NAMES.TIMESTAMP, testTable);
	}
	
	@Override
	public Object getConnection(Object factory) throws TranslatorException {
		return null;
	}

	@Override
	public boolean supportsSelectExpression() {
		return true;
	}
	
}
