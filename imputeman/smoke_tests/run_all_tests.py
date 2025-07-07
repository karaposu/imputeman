# imputeman/smoke_tests/run_all_tests.py
"""
Smoke Test Runner: Run all smoke tests in sequence
Provides a single entry point to test the entire system
"""

import asyncio
import sys
import importlib
import time
from typing import List, Tuple


class SmokeTestRunner:
    """Runner for executing all smoke tests"""
    
    def __init__(self):
        self.test_modules = [
            ("test_2_configuration_system", "Configuration System"),
            ("test_3_entities_and_schema", "Entities & Schema"),
            ("test_4_individual_services", "Individual Services"),
            ("test_1_run_imputeman_without_prefect", "Core Integration"),
            ("test_5_prefect_integration", "Prefect Integration"),
        ]
        self.results: List[Tuple[str, str, bool, float]] = []
    
    async def run_single_test(self, module_name: str, test_name: str) -> Tuple[bool, float]:
        """Run a single test module and return success status and duration"""
        
        print(f"\n{'='*20} {test_name} {'='*20}")
        start_time = time.time()
        
        try:
            # Import the test module using relative import
            module = importlib.import_module(f".{module_name}", package="imputeman.smoke_tests")
            
            # Run the main function
            if hasattr(module, 'main'):
                if asyncio.iscoroutinefunction(module.main):
                    success = await module.main()
                else:
                    success = module.main()
            else:
                print(f"❌ Test module {module_name} has no main() function")
                return False, 0.0
            
            duration = time.time() - start_time
            return success, duration
            
        except Exception as e:
            duration = time.time() - start_time
            print(f"❌ Test {test_name} crashed with error: {e}")
            import traceback
            traceback.print_exc()
            return False, duration
    
    async def run_all_tests(self) -> bool:
        """Run all smoke tests in sequence"""
        
        print("🚀 IMPUTEMAN SMOKE TEST SUITE")
        print("=" * 60)
        print("Running comprehensive tests to verify system functionality")
        print("=" * 60)
        
        total_start_time = time.time()
        all_passed = True
        
        # Run each test
        for module_name, test_name in self.test_modules:
            success, duration = await self.run_single_test(module_name, test_name)
            self.results.append((module_name, test_name, success, duration))
            
            if not success:
                all_passed = False
                print(f"\n⚠️  {test_name} FAILED - Consider fixing before proceeding")
            else:
                print(f"\n✅ {test_name} PASSED in {duration:.2f}s")
        
        total_duration = time.time() - total_start_time
        
        # Print summary
        self.print_summary(total_duration, all_passed)
        
        return all_passed
    
    def print_summary(self, total_duration: float, all_passed: bool):
        """Print test summary results"""
        
        print("\n" + "=" * 60)
        print("📊 SMOKE TEST SUMMARY")
        print("=" * 60)
        
        # Individual test results
        for module_name, test_name, success, duration in self.results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"{status:<8} {test_name:<25} ({duration:.2f}s)")
        
        # Overall summary
        passed_count = sum(1 for _, _, success, _ in self.results if success)
        total_count = len(self.results)
        
        print("-" * 60)
        print(f"Tests Passed: {passed_count}/{total_count}")
        print(f"Total Duration: {total_duration:.2f}s")
        
        if all_passed:
            print("\n🎉 ALL SMOKE TESTS PASSED!")
            print("✅ System is ready for full integration testing")
            print("✅ Core services are working correctly")
            print("✅ Configuration system is functional")
            print("✅ Entity and schema systems are operational")
        else:
            print(f"\n⚠️  {total_count - passed_count} TEST(S) FAILED")
            print("❌ Fix failing tests before proceeding to production")
            print("💡 Check error messages above for debugging guidance")
        
        # Recommendations
        print("\n📋 NEXT STEPS:")
        if all_passed:
            print("1. ✅ All core systems verified")
            print("2. 🚀 Ready to test with Prefect orchestration")
            print("3. 🔧 Integrate your actual SERP/scraping/extraction APIs")
            print("4. 🏭 Deploy to production environment")
        else:
            print("1. 🔍 Review failed test output above")
            print("2. 🛠️ Fix configuration or dependency issues") 
            print("3. 🔄 Re-run smoke tests until all pass")
            print("4. 📚 Check documentation for troubleshooting")


async def run_quick_test():
    """Run a quick subset of tests for rapid feedback"""
    
    print("🏃‍♂️ QUICK SMOKE TEST")
    print("Running essential tests only for rapid feedback")
    print("=" * 40)
    
    runner = SmokeTestRunner()
    
    # Run just the core tests
    quick_tests = [
        ("test_2_configuration_system", "Configuration"),
        ("test_3_entities_and_schema", "Entities"),
    ]
    
    start_time = time.time()
    all_passed = True
    
    for module_name, test_name in quick_tests:
        success, duration = await runner.run_single_test(module_name, test_name)
        if not success:
            all_passed = False
    
    duration = time.time() - start_time
    
    print(f"\n{'✅ QUICK TESTS PASSED' if all_passed else '❌ QUICK TESTS FAILED'}")
    print(f"Duration: {duration:.2f}s")
    
    if all_passed:
        print("💨 Core systems functional - ready for full test suite")
    
    return all_passed


def print_help():
    """Print usage help"""
    
    print("🧪 Imputeman Smoke Test Runner")
    print("=" * 40)
    print("Usage:")
    print("  python run_all_tests.py [command]")
    print("")
    print("Commands:")
    print("  all      - Run all smoke tests (default)")
    print("  quick    - Run quick essential tests only")
    print("  help     - Show this help message")
    print("")
    print("Examples:")
    print("  python run_all_tests.py")
    print("  python run_all_tests.py quick")
    print("  python run_all_tests.py all")


async def main():
    """Main entry point"""
    
    # Parse command line arguments
    command = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    if command == "help":
        print_help()
        return True
    elif command == "quick":
        return await run_quick_test()
    elif command == "all":
        runner = SmokeTestRunner()
        return await runner.run_all_tests()
    else:
        print(f"❌ Unknown command: {command}")
        print_help()
        return False


if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️ Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test runner crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)