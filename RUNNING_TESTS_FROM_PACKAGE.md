# RUNNING_TESTS_FROM_PACKAGE.md

# 🧪 Running Smoke Tests from Package Structure

Your smoke tests are inside the `imputeman` package, which is perfectly fine! Here's how to run them properly.

## 📁 Your Directory Structure:
```
imputeman/                          # ← Project root
├── imputeman/                      # ← Package
│   ├── __init__.py
│   ├── core/
│   ├── tasks/
│   ├── flows/  
│   ├── services/
│   ├── smoke_tests/                # ← Tests are here
│   │   ├── __init__.py
│   │   ├── test_1_run_imputeman_without_prefect.py
│   │   ├── test_2_configuration_system.py
│   │   ├── test_3_entities_and_schema.py
│   │   ├── test_4_individual_services.py
│   │   ├── test_5_prefect_integration.py
│   │   └── run_all_tests.py
│   └── ...
├── requirements.txt
└── setup.py
```

## ✅ **Correct Ways to Run Tests:**

### **Method 1: Module Execution (From project root)**
```bash
# Make sure you're in the project root (where setup.py is)
cd ~/Desktop/projects/imputeman

# Run individual tests
python -m imputeman.smoke_tests.test_2_configuration_system
python -m imputeman.smoke_tests.test_3_entities_and_schema
python -m imputeman.smoke_tests.test_4_individual_services

# Run all tests
python -m imputeman.smoke_tests.run_all_tests
```

### **Method 2: Direct Module Import**
```bash
# From project root
python -c "
import asyncio
from imputeman.smoke_tests.test_2_configuration_system import main
success = main()
print('✅ PASSED' if success else '❌ FAILED')
"
```

### **Method 3: Install Package & Run**
```bash
# Install in development mode
pip install -e .

# Then run tests from anywhere
python -m imputeman.smoke_tests.test_2_configuration_system
```

## 🚀 **Quick Start:**

1. **Go to project root:**
   ```bash
   cd ~/Desktop/projects/imputeman
   pwd  # Should show: /Users/ns/Desktop/projects/imputeman
   ```

2. **Quick test (should work immediately):**
   ```bash
   python -m imputeman.smoke_tests.test_2_configuration_system
   ```

3. **If that works, run all quick tests:**
   ```bash
   python -m imputeman.smoke_tests.run_all_tests quick
   ```

## 🔧 **If You Get Import Errors:**

### **Error: `ModuleNotFoundError: No module named 'imputeman'`**

**Solution 1:** Make sure you're in the right directory
```bash
cd ~/Desktop/projects/imputeman  # Go to project root
ls  # Should show: imputeman/ requirements.txt setup.py
```

**Solution 2:** Install package in development mode
```bash
pip install -e .
```

**Solution 3:** Add to Python path temporarily
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
python -m imputeman.smoke_tests.test_2_configuration_system
```

### **Error: `ModuleNotFoundError: No module named 'imputeman.smoke_tests'`**

**Solution:** Make sure you have `__init__.py` files:
```bash
ls imputeman/__init__.py                # Should exist
ls imputeman/smoke_tests/__init__.py    # Should exist
```

## 🎯 **Expected Output:**
```bash
$ python -m imputeman.smoke_tests.test_2_configuration_system

🧪 Smoke Test 2: Testing Configuration System
=============================================================

1️⃣ Testing individual config classes...
   ✅ SerpConfig: retries=3, top_k=10
   ✅ ScrapeConfig: concurrent=5, cost_threshold=$100.0
   ✅ BudgetScrapeConfig: concurrent=2, cost_threshold=$20.0
   ✅ ExtractConfig: model=gpt-4, confidence=0.7
   ✅ PipelineConfig: caching=True, max_cost=$200.0

📊 CONFIGURATION TEST RESULTS
=============================================================
✅ PASS    Config Creation       (0.1s)
🎉 All configuration tests passed!
```

## 📋 **Test Execution Order:**

### **Phase 1: Quick Foundation Tests**
```bash
python -m imputeman.smoke_tests.test_2_configuration_system  # ~5 seconds
python -m imputeman.smoke_tests.test_3_entities_and_schema   # ~5 seconds
```

### **Phase 2: Service Tests**
```bash
python -m imputeman.smoke_tests.test_4_individual_services   # ~20 seconds
```

### **Phase 3: Integration Tests**
```bash
python -m imputeman.smoke_tests.test_1_run_imputeman_without_prefect  # ~30 seconds
python -m imputeman.smoke_tests.test_5_prefect_integration             # ~45 seconds
```

### **All Tests at Once:**
```bash
python -m imputeman.smoke_tests.run_all_tests quick  # Essential tests only
python -m imputeman.smoke_tests.run_all_tests        # All tests
```

## 💡 **Pro Tips:**

1. **Always run from project root** (where setup.py is)
2. **Start with test_2** (quickest, no dependencies)
3. **Install with `pip install -e .`** to avoid path issues
4. **Tests use mock data** - no API keys needed

Your package structure is totally fine! The key is running the tests as **modules** using the `-m` flag from the project root directory.