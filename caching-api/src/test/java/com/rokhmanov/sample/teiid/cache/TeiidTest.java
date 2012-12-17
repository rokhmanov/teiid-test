package com.rokhmanov.sample.teiid.cache;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;

public class TeiidTest {
	
	private static EmbeddedServer server;
    private TeiidDriver teiidDriver;
    private Connection connection;
	
	@Test
	public void testDataCached() throws Exception
	{
		List<String> result = execute("select * from cached.test");
		String dateMark1 = result.get(0);  
		Thread.sleep(1000);
		List<String> resultCached = execute("select * from cached.test");
		String dateMark2 = resultCached.get(0);
		System.out.println("Should be taken from cache:" + dateMark1 + "----" + dateMark2);
		assertTrue(dateMark1.equalsIgnoreCase(dateMark2));
	}
	
	
	@Test
	public void testDisableCacheDynamically() throws Exception
	{
		List<String> result = execute("select * from cached.test");
		String dateMark1 = result.get(0);  
		Thread.sleep(1000);
		List<String> resultNotCached = execute("select * from cached.test where id=2");
		String dateMark2 = resultNotCached.get(0);
		System.out.println("The second result should not be taken from cache:" + dateMark1 + "----" + dateMark2);
		assertFalse(dateMark1.equalsIgnoreCase(dateMark2));
	}
	
	private List<String> execute(String sql) throws Exception {
		List<String> result = new ArrayList<String>();			
			Statement statement = connection.createStatement();
			boolean hasResults = statement.execute(sql);
			if (hasResults) {
				ResultSet rows = statement.getResultSet();
				while (rows.next())
				{
					result.add(rows.getInt(1) + "," + rows.getString(2));
				}
				rows.close();
			}
			statement.close();
		return result;
	}
	
    
	@BeforeClass
	public static void init() throws Exception
	{
        EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(true);				
		server = new EmbeddedServer();
		server.start(ec);
        server.addTranslator(new CacheTestExecutionFactory());

        final ModelMetaData jdbcCachedModel = new ModelMetaData();
        jdbcCachedModel.setName("cached");
        jdbcCachedModel.setSchemaSourceType("native");
        jdbcCachedModel.addSourceMapping("connector-cache", "test-caching", null);

		server.deployVDB("example", jdbcCachedModel);
	}
	
	
	@AfterClass
	public static void tearDown()
	{
		server.stop();
	}
	
	
	@Before
	public void prepare() throws Exception
	{
		teiidDriver = server.getDriver();
		connection = teiidDriver.connect("jdbc:teiid:example", null);		
	}
	
	
	@After
	public void release() throws Exception
	{
		if (null != connection && !connection.isClosed())
		{
			connection.close();
		}
	}
		
	
}
