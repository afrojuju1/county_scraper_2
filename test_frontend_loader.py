#!/usr/bin/env python3
"""
Test script for the new frontend loader CLI commands.
"""

import subprocess
import sys
import os

def test_cli_help():
    """Test that the CLI help shows our new commands."""
    print("🧪 Testing CLI Help Commands")
    print("=" * 50)
    
    try:
        # Test help for the main CLI
        result = subprocess.run([
            sys.executable, "-m", "county_parser.cli.main", "--help"
        ], capture_output=True, text=True, cwd="county_parser")
        
        if result.returncode == 0:
            print("✅ Main CLI help works")
            
            # Check for our new commands
            help_text = result.stdout.lower()
            commands = [
                "load_travis_sample_for_frontend",
                "load_dallas_sample_for_frontend", 
                "load_harris_sample_for_frontend"
            ]
            
            for cmd in commands:
                if cmd in help_text:
                    print(f"✅ Found command: {cmd}")
                else:
                    print(f"❌ Missing command: {cmd}")
        else:
            print(f"❌ CLI help failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error testing CLI: {e}")

def test_travis_command_help():
    """Test the Travis County frontend loader help."""
    print("\n🧪 Testing Travis County Command Help")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "county_parser.cli.main", "load-travis-sample-for-frontend", "--help"
        ], capture_output=True, text=True, cwd="county_parser")
        
        if result.returncode == 0:
            print("✅ Travis County command help works")
            print("📋 Available options:")
            
            # Parse and display options
            help_lines = result.stdout.split('\n')
            for line in help_lines:
                if line.strip().startswith('--'):
                    print(f"   {line.strip()}")
        else:
            print(f"❌ Travis command help failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error testing Travis command: {e}")

def test_dallas_command_help():
    """Test the Dallas County frontend loader help."""
    print("\n🧪 Testing Dallas County Command Help")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "county_parser.cli.main", "load-dallas-sample-for-frontend", "--help"
        ], capture_output=True, text=True, cwd="county_parser")
        
        if result.returncode == 0:
            print("✅ Dallas County command help works")
            print("📋 Available options:")
            
            # Parse and display options
            help_lines = result.stdout.split('\n')
            for line in help_lines:
                if line.strip().startswith('--'):
                    print(f"   {line.strip()}")
        else:
            print(f"❌ Dallas command help failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error testing Dallas command: {e}")

def test_harris_command_help():
    """Test the Harris County frontend loader help."""
    print("\n🧪 Testing Harris County Command Help")
    print("=" * 50)
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "county_parser.cli.main", "load-harris-sample-for-frontend", "--help"
        ], capture_output=True, text=True, cwd="county_parser")
        
        if result.returncode == 0:
            print("✅ Harris County command help works")
            print("📋 Available options:")
            
            # Parse and display options
            help_lines = result.stdout.split('\n')
            for line in help_lines:
                if line.strip().startswith('--'):
                    print(f"   {line.strip()}")
        else:
            print(f"❌ Harris command help failed: {result.stderr}")
            
    except Exception as e:
        print(f"❌ Error testing Harris command: {e}")

def main():
    """Run all tests."""
    print("🚀 Testing Frontend Loader CLI Commands")
    print("=" * 60)
    
    # Change to the county_parser directory
    if not os.path.exists("county_parser"):
        print("❌ county_parser directory not found")
        return
    
    test_cli_help()
    test_travis_command_help()
    test_dallas_command_help()
    test_harris_command_help()
    
    print("\n" + "=" * 60)
    print("🎉 CLI Command Testing Complete!")
    print("\n💡 To load data for frontend review:")
    print("   • Travis County: python -m county_parser.cli.main load-travis-sample-for-frontend --sample-size 5000")
    print("   • Dallas County: python -m county_parser.cli.main load-dallas-sample-for-frontend --sample-size 5000")
    print("   • Harris County: python -m county_parser.cli.main load-harris-sample-for-frontend --sample-size 5000")
    print("\n🌐 Then access the frontend at: http://localhost:5000")

if __name__ == "__main__":
    main()
