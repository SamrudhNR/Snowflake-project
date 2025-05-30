import React, { useState, useEffect } from "react";
import "./App.css";
import axios from "axios";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api`;

const App = () => {
  const [activeTab, setActiveTab] = useState('setup');
  const [snowflakeConnection, setSnowflakeConnection] = useState({
    account: '',
    username: '',
    password: '',
    warehouse: 'COMPUTE_WH',
    database: 'FINANCIAL_DW',
    schema: 'PUBLIC'
  });
  const [connectionStatus, setConnectionStatus] = useState('disconnected');
  const [dashboardData, setDashboardData] = useState(null);
  const [queryResult, setQueryResult] = useState(null);
  const [sqlQuery, setSqlQuery] = useState('');
  const [exampleQueries, setExampleQueries] = useState([]);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState('');

  // Check connection status on load
  useEffect(() => {
    checkConnectionStatus();
    loadExampleQueries();
  }, []);

  const checkConnectionStatus = async () => {
    try {
      const response = await axios.get(`${API}/snowflake/status`);
      setConnectionStatus(response.data.status);
    } catch (error) {
      console.error('Failed to check connection status:', error);
    }
  };

  const loadExampleQueries = async () => {
    try {
      const response = await axios.get(`${API}/examples/queries`);
      setExampleQueries(response.data.examples);
    } catch (error) {
      console.error('Failed to load example queries:', error);
    }
  };

  const connectToSnowflake = async () => {
    setLoading(true);
    try {
      const response = await axios.post(`${API}/snowflake/connect`, snowflakeConnection);
      setConnectionStatus('connected');
      setMessage('Successfully connected to Snowflake!');
    } catch (error) {
      setMessage(`Connection failed: ${error.response?.data?.detail || error.message}`);
    }
    setLoading(false);
  };

  const setupDatabase = async () => {
    setLoading(true);
    try {
      const response = await axios.post(`${API}/snowflake/setup`);
      setMessage(`Database setup complete: ${response.data.message}`);
    } catch (error) {
      setMessage(`Setup failed: ${error.response?.data?.detail || error.message}`);
    }
    setLoading(false);
  };

  const generateSampleData = async () => {
    setLoading(true);
    try {
      const response = await axios.post(`${API}/data/generate`, {
        customers: 100,
        merchants: 50,
        transactions: 1000
      });
      setMessage(`Sample data generated: ${JSON.stringify(response.data.data_counts)}`);
    } catch (error) {
      setMessage(`Data generation failed: ${error.response?.data?.detail || error.message}`);
    }
    setLoading(false);
  };

  const loadDashboardData = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/analytics/dashboard`);
      setDashboardData(response.data);
    } catch (error) {
      setMessage(`Dashboard load failed: ${error.response?.data?.detail || error.message}`);
    }
    setLoading(false);
  };

  const executeQuery = async () => {
    if (!sqlQuery.trim()) return;
    
    setLoading(true);
    try {
      const response = await axios.post(`${API}/query/execute`, {
        sql_query: sqlQuery,
        limit: 100
      });
      setQueryResult(response.data);
    } catch (error) {
      setMessage(`Query failed: ${error.response?.data?.detail || error.message}`);
    }
    setLoading(false);
  };

  const SnowflakeSetupTab = () => (
    <div className="space-y-6">
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-blue-800 mb-4">📋 Snowflake Setup Instructions</h3>
        <div className="space-y-3 text-sm text-blue-700">
          <p><strong>Don't have a Snowflake account?</strong> Follow these steps:</p>
          <ol className="list-decimal list-inside space-y-2 ml-4">
            <li>Go to <a href="https://signup.snowflake.com/" target="_blank" rel="noopener noreferrer" className="text-blue-600 underline">signup.snowflake.com</a></li>
            <li>Sign up for a <strong>30-day free trial</strong></li>
            <li>Choose any cloud provider (AWS, Azure, or GCP)</li>
            <li>Note your <strong>account identifier</strong> (e.g., abc123.us-east-1)</li>
            <li>Create username and password</li>
            <li>Use the credentials below to connect</li>
          </ol>
        </div>
      </div>

      <div className="bg-white border border-gray-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold mb-4">🔗 Snowflake Connection</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Account Identifier</label>
            <input
              type="text"
              placeholder="e.g., abc123.us-east-1"
              className="w-full px-3 py-2 border border-gray-300 rounded-md"
              value={snowflakeConnection.account}
              onChange={(e) => setSnowflakeConnection({...snowflakeConnection, account: e.target.value})}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
            <input
              type="text"
              className="w-full px-3 py-2 border border-gray-300 rounded-md"
              value={snowflakeConnection.username}
              onChange={(e) => setSnowflakeConnection({...snowflakeConnection, username: e.target.value})}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Password</label>
            <input
              type="password"
              className="w-full px-3 py-2 border border-gray-300 rounded-md"
              value={snowflakeConnection.password}
              onChange={(e) => setSnowflakeConnection({...snowflakeConnection, password: e.target.value})}
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Warehouse</label>
            <input
              type="text"
              className="w-full px-3 py-2 border border-gray-300 rounded-md"
              value={snowflakeConnection.warehouse}
              onChange={(e) => setSnowflakeConnection({...snowflakeConnection, warehouse: e.target.value})}
            />
          </div>
        </div>
        
        <div className="mt-4 flex items-center space-x-4">
          <button
            onClick={connectToSnowflake}
            disabled={loading}
            className="bg-blue-600 text-white px-6 py-2 rounded-md hover:bg-blue-700 disabled:opacity-50"
          >
            {loading ? 'Connecting...' : 'Connect to Snowflake'}
          </button>
          <span className={`px-3 py-1 rounded-full text-sm ${
            connectionStatus === 'connected' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
          }`}>
            {connectionStatus === 'connected' ? '✅ Connected' : '❌ Disconnected'}
          </span>
        </div>
      </div>

      {connectionStatus === 'connected' && (
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold mb-4">🏗️ Database Setup</h3>
          <div className="space-y-4">
            <button
              onClick={setupDatabase}
              disabled={loading}
              className="bg-green-600 text-white px-6 py-2 rounded-md hover:bg-green-700 disabled:opacity-50 mr-4"
            >
              {loading ? 'Setting up...' : 'Create Database Schema'}
            </button>
            <button
              onClick={generateSampleData}
              disabled={loading}
              className="bg-purple-600 text-white px-6 py-2 rounded-md hover:bg-purple-700 disabled:opacity-50"
            >
              {loading ? 'Generating...' : 'Generate Sample Data'}
            </button>
          </div>
          <p className="text-sm text-gray-600 mt-2">
            This will create the financial data warehouse schema and populate it with sample transaction data.
          </p>
        </div>
      )}

      {message && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4">
          <p className="text-sm text-gray-700">{message}</p>
        </div>
      )}
    </div>
  );

  const DashboardTab = () => (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h3 className="text-lg font-semibold">📊 Financial Analytics Dashboard</h3>
        <button
          onClick={loadDashboardData}
          disabled={loading || connectionStatus !== 'connected'}
          className="bg-blue-600 text-white px-4 py-2 rounded-md hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? 'Loading...' : 'Refresh Data'}
        </button>
      </div>

      {dashboardData && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h4 className="text-sm font-medium text-gray-600">Total Transactions</h4>
            <p className="text-2xl font-bold text-blue-600">{dashboardData.summary.total_transactions?.toLocaleString()}</p>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h4 className="text-sm font-medium text-gray-600">Total Volume</h4>
            <p className="text-2xl font-bold text-green-600">${dashboardData.summary.total_amount?.toLocaleString(undefined, {minimumFractionDigits: 2})}</p>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-6">
            <h4 className="text-sm font-medium text-gray-600">Average Transaction</h4>
            <p className="text-2xl font-bold text-purple-600">${dashboardData.summary.avg_transaction?.toFixed(2)}</p>
          </div>
        </div>
      )}

      {dashboardData?.category_breakdown && (
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <h4 className="text-lg font-semibold mb-4">Transaction Categories</h4>
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2">Category</th>
                  <th className="text-right py-2">Transactions</th>
                  <th className="text-right py-2">Total Amount</th>
                </tr>
              </thead>
              <tbody>
                {dashboardData.category_breakdown.data?.map((row, index) => (
                  <tr key={index} className="border-b">
                    <td className="py-2">{row[0]}</td>
                    <td className="text-right py-2">{row[1]?.toLocaleString()}</td>
                    <td className="text-right py-2">${row[2]?.toLocaleString(undefined, {minimumFractionDigits: 2})}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {dashboardData?.top_customers && (
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <h4 className="text-lg font-semibold mb-4">Top Customers</h4>
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-2">Customer</th>
                  <th className="text-right py-2">Transactions</th>
                  <th className="text-right py-2">Total Spent</th>
                </tr>
              </thead>
              <tbody>
                {dashboardData.top_customers.data?.map((row, index) => (
                  <tr key={index} className="border-b">
                    <td className="py-2">{row[0]}</td>
                    <td className="text-right py-2">{row[1]}</td>
                    <td className="text-right py-2">${row[2]?.toLocaleString(undefined, {minimumFractionDigits: 2})}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );

  const QueryTab = () => (
    <div className="space-y-6">
      <div className="bg-white border border-gray-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold mb-4">💻 SQL Query Interface</h3>
        <div className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">SQL Query</label>
            <textarea
              rows={6}
              className="w-full px-3 py-2 border border-gray-300 rounded-md font-mono text-sm"
              placeholder="Enter your SQL query here..."
              value={sqlQuery}
              onChange={(e) => setSqlQuery(e.target.value)}
            />
          </div>
          <button
            onClick={executeQuery}
            disabled={loading || connectionStatus !== 'connected' || !sqlQuery.trim()}
            className="bg-green-600 text-white px-6 py-2 rounded-md hover:bg-green-700 disabled:opacity-50"
          >
            {loading ? 'Executing...' : 'Execute Query'}
          </button>
        </div>
      </div>

      {queryResult && (
        <div className="bg-white border border-gray-200 rounded-lg p-6">
          <h4 className="text-lg font-semibold mb-4">Query Results</h4>
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead>
                <tr className="border-b bg-gray-50">
                  {queryResult.columns?.map((col, index) => (
                    <th key={index} className="text-left py-2 px-3 font-medium">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {queryResult.data?.slice(0, 100).map((row, rowIndex) => (
                  <tr key={rowIndex} className="border-b">
                    {row.map((cell, cellIndex) => (
                      <td key={cellIndex} className="py-2 px-3 text-sm">{cell?.toString()}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {queryResult.data?.length > 100 && (
            <p className="text-sm text-gray-600 mt-2">Showing first 100 rows of {queryResult.data.length} results.</p>
          )}
        </div>
      )}

      <div className="bg-white border border-gray-200 rounded-lg p-6">
        <h4 className="text-lg font-semibold mb-4">📝 Example Queries</h4>
        <div className="space-y-4">
          {exampleQueries.map((example, index) => (
            <div key={index} className="border border-gray-200 rounded-lg p-4">
              <h5 className="font-medium text-gray-800">{example.title}</h5>
              <p className="text-sm text-gray-600 mb-2">{example.description}</p>
              <pre className="bg-gray-50 p-3 rounded text-sm font-mono overflow-x-auto">{example.query}</pre>
              <button
                onClick={() => setSqlQuery(example.query)}
                className="mt-2 text-blue-600 text-sm hover:underline"
              >
                Use This Query
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gray-100">
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-6">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">🏦 Financial Data Warehouse</h1>
              <p className="text-gray-600">Snowflake-powered financial analytics and ETL pipeline</p>
            </div>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="mb-8">
          <nav className="flex space-x-8">
            {[
              { id: 'setup', label: '🔧 Setup', icon: '🔧' },
              { id: 'dashboard', label: '📊 Dashboard', icon: '📊' },
              { id: 'query', label: '💻 Query', icon: '💻' }
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-2 px-1 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>

        <div className="bg-white rounded-lg shadow-sm p-6">
          {activeTab === 'setup' && <SnowflakeSetupTab />}
          {activeTab === 'dashboard' && <DashboardTab />}
          {activeTab === 'query' && <QueryTab />}
        </div>
      </div>
    </div>
  );
};

export default App;
