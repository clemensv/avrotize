#!/usr/bin/env python3
"""
Coverage Analysis Script for Avrotize C# Code Generation
Generates multiple test projects and analyzes their code coverage patterns.
"""

import os
import tempfile
import subprocess
import sys
import shutil
import json
import re
from pathlib import Path

# Add the project root to the path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

from avrotize.avrotocsharp import convert_avro_to_csharp

def run_coverage_analysis():
    """Run comprehensive coverage analysis across different test scenarios."""
    
    # Test scenarios to analyze
    test_cases = [
        {
            'name': 'address-basic', 
            'schema': 'test/avsc/address.avsc', 
            'annotations': {},
            'description': 'Basic address schema without annotations'
        },
        {
            'name': 'address-json', 
            'schema': 'test/avsc/address.avsc', 
            'annotations': {'system_text_json_annotation': True},
            'description': 'Address schema with System.Text.Json annotations'
        },
        {
            'name': 'address-full', 
            'schema': 'test/avsc/address.avsc', 
            'annotations': {
                'system_text_json_annotation': True, 
                'newtonsoft_json_annotation': True, 
                'system_xml_annotation': True, 
                'pascal_properties': True
            },
            'description': 'Address schema with all annotations enabled'
        },
        {
            'name': 'enumfield-basic', 
            'schema': 'test/avsc/enumfield.avsc', 
            'annotations': {},
            'description': 'Enum field schema without annotations'
        },
        {
            'name': 'telemetry-basic', 
            'schema': 'test/avsc/telemetry.avsc', 
            'annotations': {},
            'description': 'Telemetry schema without annotations'
        },
    ]

    base_dir = os.path.join(tempfile.gettempdir(), 'avrotize', 'coverage-analysis')
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)

    print('🔬 Starting comprehensive coverage analysis...')
    print('=' * 60)
    
    results = []
    
    for case in test_cases:
        print(f"\\n📊 Analyzing: {case['name']}")
        print(f"📝 Description: {case['description']}")
        
        try:
            # Generate the C# project
            output_dir = os.path.join(base_dir, case['name'])
            namespace = f"CoverageTest.{case['name'].replace('-', '')}"
            project_name = case['name'].replace('-', '')
            
            convert_avro_to_csharp(
                case['schema'], 
                output_dir,
                base_namespace=namespace,
                project_name=project_name,
                **case['annotations']
            )
            print(f"✅ Generated project: {case['name']}")
            
            # Run coverage analysis
            coverage_result = run_coverage_for_project(output_dir, case['name'])
            if coverage_result:
                results.append({
                    'name': case['name'],
                    'description': case['description'],
                    'schema': case['schema'],
                    'annotations': case['annotations'],
                    'coverage': coverage_result
                })
                print(f"📈 Line Coverage: {coverage_result['line_coverage']:.2f}%")
                print(f"🌿 Branch Coverage: {coverage_result['branch_coverage']:.2f}%")
            else:
                print("❌ Coverage analysis failed")
                
        except Exception as e:
            print(f"❌ Failed to analyze {case['name']}: {e}")
            continue
    
    # Generate summary report
    print('\\n' + '=' * 60)
    print('📊 COVERAGE ANALYSIS SUMMARY')
    print('=' * 60)
    
    if results:
        generate_summary_report(results)
    else:
        print("❌ No successful coverage results to report")
    
    return results

def run_coverage_for_project(project_dir, name):
    """Run coverage analysis for a specific project."""
    try:
        # Change to project directory
        original_dir = os.getcwd()
        os.chdir(project_dir)
        
        # Run tests with coverage
        result = subprocess.run([
            'dotnet', 'test', 
            '--collect:XPlat Code Coverage',
            '--results-directory:./coverage',
            '--verbosity:quiet'
        ], capture_output=True, text=True, timeout=120)
        
        if result.returncode != 0:
            print(f"⚠️ Test execution had issues: {result.stderr}")
        
        # Find and parse coverage file
        coverage_file = find_coverage_file(project_dir)
        if coverage_file:
            return parse_cobertura_coverage(coverage_file)
        else:
            print("❌ No coverage file found")
            return None
            
    except subprocess.TimeoutExpired:
        print("⏰ Test execution timed out")
        return None
    except Exception as e:
        print(f"❌ Error running coverage: {e}")
        return None
    finally:
        os.chdir(original_dir)

def find_coverage_file(project_dir):
    """Find the Cobertura coverage XML file."""
    coverage_dir = os.path.join(project_dir, 'coverage')
    if not os.path.exists(coverage_dir):
        return None
    
    for root, dirs, files in os.walk(coverage_dir):
        for file in files:
            if file.endswith('coverage.cobertura.xml'):
                return os.path.join(root, file)
    return None

def parse_cobertura_coverage(coverage_file):
    """Parse Cobertura XML coverage file to extract metrics."""
    try:
        with open(coverage_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Extract line rate and branch rate using regex
        line_match = re.search(r'line-rate="([0-9.]+)"', content)
        branch_match = re.search(r'branch-rate="([0-9.]+)"', content)
        
        if line_match and branch_match:
            line_rate = float(line_match.group(1)) * 100
            branch_rate = float(branch_match.group(1)) * 100
            
            return {
                'line_coverage': line_rate,
                'branch_coverage': branch_rate,
                'coverage_file': coverage_file
            }
    except Exception as e:
        print(f"❌ Error parsing coverage file: {e}")
    
    return None

def generate_summary_report(results):
    """Generate a comprehensive summary report."""
    
    # Sort results by line coverage
    results_sorted = sorted(results, key=lambda x: x['coverage']['line_coverage'], reverse=True)
    
    print("\\n📋 Coverage Results by Line Coverage:")
    print("-" * 80)
    print(f"{'Schema':<20} {'Config':<15} {'Line %':<8} {'Branch %':<10} {'Description'}")
    print("-" * 80)
    
    total_line = 0
    total_branch = 0
    
    for result in results_sorted:
        schema_name = os.path.basename(result['schema']).replace('.avsc', '')
        config = get_config_summary(result['annotations'])
        line_cov = result['coverage']['line_coverage']
        branch_cov = result['coverage']['branch_coverage']
        
        total_line += line_cov
        total_branch += branch_cov
        
        print(f"{schema_name:<20} {config:<15} {line_cov:>6.2f}% {branch_cov:>8.2f}% {result['description'][:40]}")
    
    print("-" * 80)
    avg_line = total_line / len(results) if results else 0
    avg_branch = total_branch / len(results) if results else 0
    print(f"{'AVERAGE':<20} {'':<15} {avg_line:>6.2f}% {avg_branch:>8.2f}%")
    
    # Analysis insights
    print("\\n🔍 Analysis Insights:")
    print("-" * 40)
    
    best_line = max(results, key=lambda x: x['coverage']['line_coverage'])
    worst_line = min(results, key=lambda x: x['coverage']['line_coverage'])
    
    print(f"🥇 Best Line Coverage: {best_line['name']} ({best_line['coverage']['line_coverage']:.2f}%)")
    print(f"🔴 Lowest Line Coverage: {worst_line['name']} ({worst_line['coverage']['line_coverage']:.2f}%)")
    
    # Identify patterns
    basic_results = [r for r in results if not r['annotations']]
    annotated_results = [r for r in results if r['annotations']]
    
    if basic_results and annotated_results:
        basic_avg = sum(r['coverage']['line_coverage'] for r in basic_results) / len(basic_results)
        annotated_avg = sum(r['coverage']['line_coverage'] for r in annotated_results) / len(annotated_results)
        
        print(f"\\n📊 Pattern Analysis:")
        print(f"   Basic schemas average: {basic_avg:.2f}% line coverage")
        print(f"   Annotated schemas average: {annotated_avg:.2f}% line coverage")
        
        if basic_avg > annotated_avg:
            diff = basic_avg - annotated_avg
            print(f"   💡 Basic schemas have {diff:.2f}% higher coverage")
            print(f"      This suggests annotation-generated code needs additional tests")
        else:
            diff = annotated_avg - basic_avg
            print(f"   💡 Annotated schemas have {diff:.2f}% higher coverage")

def get_config_summary(annotations):
    """Get a short summary of annotation configuration."""
    if not annotations:
        return "Basic"
    
    enabled = []
    if annotations.get('system_text_json_annotation'):
        enabled.append("JSON")
    if annotations.get('newtonsoft_json_annotation'):
        enabled.append("Newt")
    if annotations.get('system_xml_annotation'):
        enabled.append("XML")
    if annotations.get('pascal_properties'):
        enabled.append("Pascal")
    if annotations.get('avro_annotation'):
        enabled.append("Avro")
    
    return "+".join(enabled) if enabled else "Basic"

if __name__ == "__main__":
    try:
        results = run_coverage_analysis()
        print(f"\\n✅ Coverage analysis complete! Generated {len(results)} results.")
    except KeyboardInterrupt:
        print("\\n⏹️ Analysis interrupted by user")
    except Exception as e:
        print(f"\\n❌ Analysis failed: {e}")
        sys.exit(1)