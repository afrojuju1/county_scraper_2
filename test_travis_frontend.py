#!/usr/bin/env python3
"""
Test script for Travis County frontend loader functionality.
This tests the core functionality before scaling to multi-county loading.
"""

import subprocess
import sys
import os
import time

def test_travis_command_help():
    """Test that the Travis County command help works."""
    print("🧪 Testing Travis County Command Help")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "county_parser", "load-travis-sample-for-frontend", "--help"
        ], capture_output=True, text=True, cwd=".")
        
        if result.returncode == 0:
            print("✅ Travis County command help works")
            print("📋 Available options:")
            
            # Parse and display options
            help_lines = result.stdout.split('\n')
            for line in help_lines:
                if line.strip().startswith('--'):
                    print(f"   {line.strip()}")
            return True
        else:
            print(f"❌ Travis command help failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Travis command: {e}")
        return False

def test_travis_small_sample():
    """Test with a small sample size to verify the pipeline works."""
    print("\n🧪 Testing Travis County Small Sample (10 properties)")
    print("=" * 50)
    
    try:
        print("🚀 Starting Travis County sample load...")
        print("   This will test the full pipeline with a small sample")
        
        # Run with small sample size for testing
        result = subprocess.run([
            sys.executable, "-m", "county_parser", "load-travis-sample-for-frontend",
            "--sample-size", "10"
        ], capture_output=True, text=True, cwd=".")
        
        if result.returncode == 0:
            print("✅ Travis County sample load completed successfully!")
            print("📊 Output:")
            print(result.stdout)
            return True
        else:
            print(f"❌ Travis County sample load failed: {result.stderr}")
            print("📊 Partial output:")
            print(result.stdout)
            return False
            
    except Exception as e:
        print(f"❌ Error during Travis County sample load: {e}")
        return False

def test_mongodb_connection():
    """Test MongoDB connection to ensure the service is ready."""
    print("\n🧪 Testing MongoDB Connection")
    print("=" * 50)
    
    try:
        # Test MongoDB status command
        result = subprocess.run([
            sys.executable, "-m", "county_parser", "mongodb-status"
        ], capture_output=True, text=True, cwd=".")
        
        if result.returncode == 0:
            print("✅ MongoDB connection successful")
            print("📊 Status:")
            print(result.stdout)
            return True
        else:
            print(f"❌ MongoDB connection failed: {result.stderr}")
            print("⚠️  Make sure MongoDB is running and accessible")
            return False
            
    except Exception as e:
        print(f"❌ Error testing MongoDB connection: {e}")
        return False

def show_scalability_plan():
    """Show how we can scale this to handle all counties simultaneously."""
    print("\n🚀 Scalability Plan for Multi-County Loading")
    print("=" * 60)
    
    print("📋 Current Implementation:")
    print("   • Individual commands for each county")
    print("   • Sequential processing")
    print("   • Separate error handling per county")
    
    print("\n🔧 Scalability Improvements:")
    print("   • Parallel processing with multiprocessing")
    print("   • Unified batch management")
    print("   • Shared MongoDB connection pool")
    print("   • Progress tracking across all counties")
    print("   • Rollback capability if any county fails")
    
    print("\n💡 Future Multi-County Command Structure:")
    print("   python -m county_parser load-all-counties-for-frontend \\")
    print("     --travis-size 5000 \\")
    print("     --dallas-size 5000 \\")
    print("     --harris-size 5000 \\")
    print("     --parallel \\")
    print("     --batch-id multi_county_batch_001")
    
    print("\n🔄 Benefits of Scalable Approach:")
    print("   • Faster total processing time")
    print("   • Better resource utilization")
    print("   • Unified progress reporting")
    print("   • Atomic batch operations")
    print("   • Easier monitoring and debugging")

def main():
    """Run Travis County focused testing."""
    print("🏛️ Travis County Frontend Loader Testing")
    print("=" * 60)
    print("🎯 Focus: Testing Travis County functionality before scaling")
    print("📊 Goal: Verify the pipeline works with real data")
    
    # Test 1: Command help
    help_works = test_travis_command_help()
    
    # Test 2: MongoDB connection
    mongodb_works = test_mongodb_connection()
    
    # Test 3: Small sample load (only if MongoDB is working)
    sample_works = False
    if mongodb_works:
        sample_works = test_travis_small_sample()
    else:
        print("\n⚠️  Skipping sample load test - MongoDB not accessible")
    
    # Results summary
    print("\n" + "=" * 60)
    print("📊 Test Results Summary")
    print("=" * 60)
    print(f"   Command Help: {'✅ PASS' if help_works else '❌ FAIL'}")
    print(f"   MongoDB: {'✅ PASS' if mongodb_works else '❌ FAIL'}")
    print(f"   Sample Load: {'✅ PASS' if sample_works else '❌ FAIL'}")
    
    if help_works and mongodb_works and sample_works:
        print("\n🎉 All tests passed! Travis County loader is ready.")
        print("💡 Next step: Scale to handle all counties simultaneously")
    elif help_works and mongodb_works:
        print("\n⚠️  Basic functionality works, but sample loading needs attention")
        print("💡 Check Travis County data files and normalizer")
    elif help_works:
        print("\n⚠️  Command structure is correct, but MongoDB connection needed")
        print("💡 Start MongoDB service and ensure connection details are correct")
    else:
        print("\n❌ Basic command structure has issues")
        print("💡 Check CLI command registration and imports")
    
    # Show scalability plan
    show_scalability_plan()
    
    print("\n" + "=" * 60)
    print("💡 To test Travis County loader manually:")
    print("   python -m county_parser load-travis-sample-for-frontend --sample-size 100")
    print("\n🌐 Then access frontend at: http://localhost:5000")

if __name__ == "__main__":
    main()
