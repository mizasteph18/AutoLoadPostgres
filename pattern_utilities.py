#!/usr/bin/env python3
"""
Pattern Extraction & Testing Utility for PostgreSQL Data Loader
Standalone script for extracting and testing file patterns
"""

import re
import sys
from datetime import datetime
from typing import Optional, Tuple
from pathlib import Path

def extract_pattern_and_date_format(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Enhanced pattern extraction with better date detection and flexible patterns.
    """
    # Enhanced date patterns with more flexibility
    date_patterns = [
        # YYYYMMDD patterns
        (r'(\d{4})(\d{2})(\d{2})', '%Y%m%d'),
        (r'(\d{4})-(\d{2})-(\d{2})', '%Y-%m-%d'),
        (r'(\d{4})_(\d{2})_(\d{2})', '%Y_%m_%d'),
        (r'(\d{4})\.(\d{2})\.(\d{2})', '%Y.%m.%d'),
        
        # MMDDYYYY patterns
        (r'(\d{2})(\d{2})(\d{4})', '%m%d%Y'),
        (r'(\d{2})-(\d{2})-(\d{4})', '%m-%d-%Y'),
        (r'(\d{2})/(\d{2})/(\d{4})', '%m/%d/%Y'),
        
        # DDMMYYYY patterns
        (r'(\d{2})(\d{2})(\d{4})', '%d%m%Y'),
        (r'(\d{2})-(\d{2})-(\d{4})', '%d-%m-%Y'),
        
        # YYMMDD patterns
        (r'(\d{2})(\d{2})(\d{2})', '%y%m%d'),
        (r'(\d{2})-(\d{2})-(\d{2})', '%y-%m-%d'),
    ]

    for date_regex, date_format in date_patterns:
        match = re.search(date_regex, filename)
        if match:
            try:
                # Test if the date is valid
                date_str = ''.join(match.groups())
                clean_format = date_format.replace('-', '').replace('/', '').replace('_', '').replace('.', '')
                datetime.strptime(date_str, clean_format)
                
                # Generate a more flexible pattern
                pattern_parts = []
                last_end = 0
                
                for i, group in enumerate(match.groups()):
                    start = match.start(i + 1)
                    end = match.end(i + 1)
                    
                    # Add text before this group
                    if start > last_end:
                        text_before = filename[last_end:start]
                        # Escape special regex characters but keep common separators
                        text_before = re.sub(r'([^a-zA-Z0-9\-_\.])', r'\\\1', text_before)
                        pattern_parts.append(text_before)
                    
                    # Add the digit group pattern
                    pattern_parts.append(f'(\\d{{{len(group)}}})')
                    last_end = end
                
                # Add remaining text after last group
                if last_end < len(filename):
                    text_after = filename[last_end:]
                    text_after = re.sub(r'([^a-zA-Z0-9\-_\.])', r'\\\1', text_after)
                    pattern_parts.append(text_after)
                
                pattern = '^' + ''.join(pattern_parts) + '$'
                return pattern, date_format
                
            except ValueError:
                continue

    return None, None

def test_pattern_on_filename(pattern: str, filename: str, date_format: Optional[str] = None) -> bool:
    """
    Enhanced pattern testing with better validation.
    """
    try:
        match = re.match(pattern, filename)
        if not match:
            return False

        if date_format and match.groups():
            # Extract date parts and validate
            date_str = ''.join(match.groups())
            clean_format = date_format.replace('-', '').replace('/', '').replace('_', '').replace('.', '')
            
            # Validate date
            parsed_date = datetime.strptime(date_str, clean_format)
            
            # Additional validation: check if date is reasonable (not in distant future/past)
            current_year = datetime.now().year
            if parsed_date.year > current_year + 10 or parsed_date.year < current_year - 10:
                return False
                
            return True

        return True
        
    except (ValueError, AttributeError):
        return False

def generate_basic_pattern(filename: str) -> str:
    """
    Generate a basic pattern by replacing all digit sequences with \\d+
    """
    # Replace all consecutive digits with \d+
    basic_pattern = re.sub(r'\d+', r'\\d+', filename)
    # Escape other special characters
    basic_pattern = re.sub(r'([^a-zA-Z0-9\\])', r'\\\1', basic_pattern)
    return '^' + basic_pattern + '$'

def analyze_filename(filename: str) -> dict:
    """
    Comprehensive analysis of a filename for pattern extraction.
    """
    result = {
        'filename': filename,
        'has_extension': '.' in filename,
        'extension': Path(filename).suffix if '.' in filename else None,
        'digit_sequences': [],
        'suggested_patterns': []
    }
    
    # Find all digit sequences
    digit_sequences = list(re.finditer(r'\d+', filename))
    result['digit_sequences'] = [{
        'sequence': match.group(),
        'start': match.start(),
        'end': match.end(),
        'length': len(match.group())
    } for match in digit_sequences]
    
    # Try automatic pattern extraction
    pattern, date_format = extract_pattern_and_date_format(filename)
    if pattern and date_format:
        result['suggested_patterns'].append({
            'type': 'AUTOMATIC',
            'pattern': pattern,
            'date_format': date_format,
            'is_valid': test_pattern_on_filename(pattern, filename, date_format)
        })
    
    # Generate basic pattern
    basic_pattern = generate_basic_pattern(filename)
    result['suggested_patterns'].append({
        'type': 'BASIC',
        'pattern': basic_pattern,
        'date_format': None,
        'is_valid': test_pattern_on_filename(basic_pattern, filename)
    })
    
    # Generate common patterns based on digit sequences
    if len(digit_sequences) == 1:
        seq = digit_sequences[0]
        if seq['length'] == 8:  # Likely YYYYMMDD or MMDDYYYY
            # Try YYYYMMDD pattern
            yyyymmdd_pattern = filename[:seq['start']] + r'(\d{4})(\d{2})(\d{2})' + filename[seq['end']:]
            yyyymmdd_pattern = '^' + re.sub(r'([^a-zA-Z0-9\(\)])', r'\\\1', yyyymmdd_pattern) + '$'
            result['suggested_patterns'].append({
                'type': 'YYYYMMDD',
                'pattern': yyyymmdd_pattern,
                'date_format': '%Y%m%d',
                'is_valid': test_pattern_on_filename(yyyymmdd_pattern, filename, '%Y%m%d')
            })
    
    return result

def print_analysis_result(analysis: dict):
    """
    Print the results of filename analysis in a user-friendly format.
    """
    print(f"\n{'='*60}")
    print(f"Filename Analysis: {analysis['filename']}")
    print(f"{'='*60}")
    
    print(f"\n📁 File Information:")
    print(f"   Extension: {analysis['extension'] or 'None'}")
    
    if analysis['digit_sequences']:
        print(f"\n🔢 Digit Sequences Found:")
        for seq in analysis['digit_sequences']:
            print(f"   - '{seq['sequence']}' (position {seq['start']}-{seq['end']}, length: {seq['length']})")
    else:
        print(f"\n❌ No digit sequences found")
    
    print(f"\n🎯 Suggested Patterns:")
    for i, pattern_info in enumerate(analysis['suggested_patterns'], 1):
        status = "✅ VALID" if pattern_info['is_valid'] else "❌ INVALID"
        print(f"\n   {i}. {pattern_info['type']} Pattern {status}")
        print(f"      Pattern: {pattern_info['pattern']}")
        if pattern_info['date_format']:
            print(f"      Date Format: {pattern_info['date_format']}")
    
    # Provide usage example for the best pattern
    valid_patterns = [p for p in analysis['suggested_patterns'] if p['is_valid']]
    if valid_patterns:
        best_pattern = valid_patterns[0]
        print(f"\n💡 Usage Example:")
        print(f"   file_pattern: \"{best_pattern['pattern'][1:-1]}\"")
        if best_pattern['date_format']:
            print(f"   date_format: \"{best_pattern['date_format']}\"")
    else:
        print(f"\n⚠️  No valid patterns found automatically.")
        print(f"   Consider creating a custom pattern based on your filename structure.")

def test_specific_pattern(pattern: str, filename: str, date_format: str = None):
    """
    Test a specific pattern against a filename.
    """
    print(f"\n{'='*60}")
    print(f"Pattern Testing")
    print(f"{'='*60}")
    print(f"Pattern: {pattern}")
    print(f"Filename: {filename}")
    if date_format:
        print(f"Date Format: {date_format}")
    
    is_valid = test_pattern_on_filename(pattern, filename, date_format)
    
    if is_valid:
        print(f"\n✅ Pattern validation: PASS")
        match = re.match(pattern, filename)
        if match and match.groups():
            print(f"   Captured groups: {match.groups()}")
    else:
        print(f"\n❌ Pattern validation: FAIL")
        
    return is_valid

def main():
    """
    Main function for pattern extraction utility.
    """
    if len(sys.argv) < 2:
        print("Pattern Extraction & Testing Utility for PostgreSQL Data Loader")
        print("=" * 60)
        print("Usage:")
        print("  python pattern_utils.py <filename1> [filename2 ...]")
        print("  python pattern_utils.py --test '<pattern>' '<filename>' [date_format]")
        print("\nExamples:")
        print("  Analysis: python pattern_utils.py ghy_20250505.xlsx")
        print("  Analysis: python pattern_utils.py sales_2023-01-15.csv inventory_20230215.xlsx")
        print("  Testing:  python pattern_utils.py --test '^ghy_(\\d{4})(\\d{2})(\\d{2})\\.xlsx$' 'ghy_20250505.xlsx' '%Y%m%d'")
        print("\nThis utility will analyze filenames and suggest appropriate patterns")
        print("for use in the PostgreSQL Data Loader configuration.")
        sys.exit(1)
    
    # Check if we're in testing mode
    if sys.argv[1] == '--test':
        if len(sys.argv) < 4:
            print("Testing mode requires pattern and filename")
            print("Usage: python pattern_utils.py --test '<pattern>' '<filename>' [date_format]")
            sys.exit(1)
        
        pattern = sys.argv[2]
        filename = sys.argv[3]
        date_format = sys.argv[4] if len(sys.argv) > 4 else None
        test_specific_pattern(pattern, filename, date_format)
        
    else:
        # Analysis mode
        filenames = sys.argv[1:]
        
        for filename in filenames:
            try:
                analysis = analyze_filename(filename)
                print_analysis_result(analysis)
            except Exception as e:
                print(f"\n❌ Error analyzing {filename}: {e}")
        
        print(f"\n{'='*60}")
        print("Pattern Analysis Complete!")
        print("=" * 60)

if __name__ == "__main__":
    main()