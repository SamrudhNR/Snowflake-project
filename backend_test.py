import requests
import unittest
import json
from datetime import datetime

class FinancialDataWarehouseAPITest(unittest.TestCase):
    def setUp(self):
        self.base_url = "https://384b0b04-8c48-47c4-9b60-b8388e6046d5.preview.emergentagent.com/api"
        self.tests_run = 0
        self.tests_passed = 0

    def test_01_root_endpoint(self):
        """Test the root API endpoint"""
        print("\n🔍 Testing root endpoint...")
        try:
            response = requests.get(f"{self.base_url}/")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["message"], "Financial Data Warehouse API")
            print("✅ Root endpoint test passed")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Root endpoint test failed: {str(e)}")
        self.tests_run += 1

    def test_02_snowflake_status(self):
        """Test the Snowflake connection status endpoint"""
        print("\n🔍 Testing Snowflake connection status...")
        try:
            response = requests.get(f"{self.base_url}/snowflake/status")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("status", data)
            print(f"✅ Snowflake status test passed - Current status: {data['status']}")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Snowflake status test failed: {str(e)}")
        self.tests_run += 1

    def test_03_example_queries(self):
        """Test the example queries endpoint"""
        print("\n🔍 Testing example queries endpoint...")
        try:
            response = requests.get(f"{self.base_url}/examples/queries")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("examples", data)
            self.assertTrue(len(data["examples"]) > 0)
            print(f"✅ Example queries test passed - Found {len(data['examples'])} example queries")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Example queries test failed: {str(e)}")
        self.tests_run += 1

    def test_04_snowflake_connect_without_credentials(self):
        """Test the Snowflake connect endpoint with invalid credentials"""
        print("\n🔍 Testing Snowflake connect with invalid credentials...")
        try:
            # Test with empty credentials
            response = requests.post(
                f"{self.base_url}/snowflake/connect",
                json={
                    "account": "",
                    "username": "",
                    "password": "",
                    "warehouse": "COMPUTE_WH",
                    "database": "FINANCIAL_DW",
                    "schema": "PUBLIC"
                }
            )
            # Should return 400 Bad Request
            self.assertEqual(response.status_code, 400)
            print("✅ Snowflake connect with invalid credentials test passed - Properly rejected empty credentials")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Snowflake connect with invalid credentials test failed: {str(e)}")
        self.tests_run += 1

    def test_05_execute_query_without_connection(self):
        """Test executing a query without an active Snowflake connection"""
        print("\n🔍 Testing query execution without connection...")
        try:
            response = requests.post(
                f"{self.base_url}/query/execute",
                json={
                    "sql_query": "SELECT * FROM transactions LIMIT 10",
                    "limit": 10
                }
            )
            # Should return 400 Bad Request
            self.assertEqual(response.status_code, 400)
            data = response.json()
            self.assertIn("detail", data)
            self.assertIn("No active Snowflake connection", data["detail"])
            print("✅ Query execution without connection test passed - Properly rejected query")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Query execution without connection test failed: {str(e)}")
        self.tests_run += 1

    def test_06_dashboard_data_without_connection(self):
        """Test getting dashboard data without an active Snowflake connection"""
        print("\n🔍 Testing dashboard data without connection...")
        try:
            response = requests.get(f"{self.base_url}/analytics/dashboard")
            # Should return 400 Bad Request
            self.assertEqual(response.status_code, 400)
            data = response.json()
            self.assertIn("detail", data)
            self.assertIn("No active Snowflake connection", data["detail"])
            print("✅ Dashboard data without connection test passed - Properly rejected request")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Dashboard data without connection test failed: {str(e)}")
        self.tests_run += 1

    def test_07_setup_database_without_connection(self):
        """Test setting up database without an active Snowflake connection"""
        print("\n🔍 Testing database setup without connection...")
        try:
            response = requests.post(f"{self.base_url}/snowflake/setup")
            # Should return 400 Bad Request
            self.assertEqual(response.status_code, 400)
            data = response.json()
            self.assertIn("detail", data)
            self.assertIn("No active Snowflake connection", data["detail"])
            print("✅ Database setup without connection test passed - Properly rejected request")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Database setup without connection test failed: {str(e)}")
        self.tests_run += 1

    def test_08_generate_data_without_connection(self):
        """Test generating data without an active Snowflake connection"""
        print("\n🔍 Testing data generation without connection...")
        try:
            response = requests.post(
                f"{self.base_url}/data/generate",
                json={
                    "customers": 10,
                    "merchants": 5,
                    "transactions": 100
                }
            )
            # Should return 400 Bad Request
            self.assertEqual(response.status_code, 400)
            data = response.json()
            self.assertIn("detail", data)
            self.assertIn("No active Snowflake connection", data["detail"])
            print("✅ Data generation without connection test passed - Properly rejected request")
            self.tests_passed += 1
        except Exception as e:
            print(f"❌ Data generation without connection test failed: {str(e)}")
        self.tests_run += 1

    def tearDown(self):
        pass

if __name__ == "__main__":
    tester = FinancialDataWarehouseAPITest()
    
    # Run all tests
    test_methods = [method for method in dir(tester) if method.startswith('test_')]
    for method in sorted(test_methods):
        getattr(tester, method)()
    
    # Print results
    print(f"\n📊 Tests passed: {tester.tests_passed}/{tester.tests_run}")
    print(f"Test completion rate: {(tester.tests_passed/tester.tests_run)*100:.2f}%")