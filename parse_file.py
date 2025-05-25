#!/usr/bin/env python
import re
import csv
import io
import sys
from typing import List, Dict, Tuple, Optional
from rapidfuzz import fuzz, process

from standard_events import STANDARD_EVENTS
from standard_schools import STANDARD_SCHOOLS

def normalize_event(event_name: str, review_bool: bool) -> Tuple[str, bool]:
    """
    Normalize the event name to the closest standardized name using fuzzy matching.
    
    Args:
        event_name (str): The event name to normalize.
        review_bool (bool): The review flag to mark if the event name is missing or invalid.
    
    Returns:
        tuple: (standardized event name, review flag)
    """
    # Clean up the event name
    event_name = event_name.strip()
    
    # Initialize best match and score variables
    best_match = ""
    best_score = 0

    for standard_event, alternatives in STANDARD_EVENTS.items():
        # Combine standard event and alternatives for fuzzy matching
        possible_matches = [standard_event] + alternatives
        result = process.extractOne(event_name, possible_matches)

        # If score is higher than previous best, update with standardized
        # event name and score
        if result and result[1] > best_score:
            best_match, best_score = standard_event, result[1]

    # At end of loop, return best match if score is above threshold (80)
    if best_score > 80:
        return best_match, review_bool
    else:
        # Return original if no close match found
        return event_name, True

def normalize_school(school_name: str, review_bool: bool) -> Tuple[str, bool]:
    """
    Normalize the school name to the closest standardized name using fuzzy matching.
    
    Args:
        school_name (str): The school name to normalize.
        review_bool (bool): The review flag to mark if the school name is missing or invalid.
    
    Returns:
        tuple: (standardized school name, review flag)
    """
    # Initialize best match and score variables
    best_match = ""
    best_score = 0

    for standard_school, alternatives in STANDARD_SCHOOLS.items():
        # Combine standard school and alternatives for fuzzy matching
        possible_matches = [standard_school] + alternatives
        result = process.extractOne(school_name, possible_matches, scorer=fuzz.partial_ratio)

        # If score is higher than previous best, update with standardized 
        # school name and score
        if result and result[1] > best_score:
            best_match, best_score = standard_school, result[1]

    # At end of loop, return best match if score is above threshold (85)
    if best_score > 85:
        return best_match, review_bool
    else:
        # Return original if no close match found
        return school_name, True

def parse_name(full_name: str) -> Tuple[str, str]:
    """
    Split a full name into last and first names.
    
    Handles both formats:
    - 'last name, first name'
    - 'first name last name'
    
    Args:
        full_name (str): The full name to parse.
        
    Returns:
        tuple: (last_name, first_name)
    """
    # Check if the name contains a comma (last name, first name format)
    if ',' in full_name:
        # Remove leading and trailing whitespace and split by the comma
        parts = full_name.split(',', 1)
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
    
    # Split by spaces for first name last name format
    parts = full_name.split()
    
    # Return the first name as the first word and last name as everything else
    if len(parts) > 1:
        return " ".join(parts[1:]), parts[0]
    elif len(parts) == 1:
        return parts[0], ""
    
    return "", ""

def extract_gender_from_event(event_line: str) -> str:
    """
    Extract gender from event line.
    
    Args:
        event_line (str): The event line containing gender information.
        
    Returns:
        str: "M" for male/boys, "F" for female/girls, or "" if not found.
    """
    line_lower = event_line.lower()
    if any(word in line_lower for word in ["boys", "boy", "men", "male"]):
        return "M"
    elif any(word in line_lower for word in ["girls", "girl", "women", "female"]):
        return "F"
    return ""

def extract_round_from_line(line: str) -> str:
    """
    Extract round information from a line.
    
    Args:
        line (str): The line to check for round information.
        
    Returns:
        str: The round type or empty string if not found.
    """
    line_lower = line.lower()
    if "final" in line_lower and "semi" not in line_lower:
        return "Final"
    elif "semi" in line_lower:
        return "Semi-Final"
    elif "prelim" in line_lower:
        return "Prelim"
    return ""

def identify_column_order(header_line: str) -> Dict[str, int]:
    """
    Identify the column order from header line.
    
    Args:
        header_line (str): The header line containing column names.
        
    Returns:
        dict: Mapping of column types to their positions.
    """
    print(f"Analyzing header line: {header_line}")
    
    # Split the header line by whitespace while preserving positions
    headers = header_line.lower().split()
    column_map = {}
    
    for i, header in enumerate(headers):
        # Clean header of common symbols
        clean_header = header.strip('()[]{}#')
        
        if clean_header in ["place", "pl", "pos", "position"]:
            column_map["place"] = i
        elif clean_header in ["name", "athlete", "competitor"]:
            column_map["name"] = i
        elif clean_header in ["school", "affiliation", "team", "club"]:
            column_map["school"] = i
        elif clean_header in ["time", "mark", "finals", "result", "performance"]:
            column_map["mark"] = i
        elif clean_header in ["heat", "ht", "h#", "flight", "fl"]:
            column_map["heat"] = i
        elif clean_header in ["wind", "w", "wind.", "wnd"]:
            column_map["wind"] = i
        elif clean_header in ["points", "pts", "pt", "score"]:
            column_map["points"] = i
        elif clean_header in ["year", "grade", "yr", "class"]:
            column_map["grade"] = i
        elif clean_header in ["lane", "ln"]:
            column_map["lane"] = i
    
    print(f"Column map identified: {column_map}")
    return column_map

def is_header_line(line: str) -> bool:
    """
    Determine if a line is a header line containing column names.
    
    Args:
        line (str): The line to check.
        
    Returns:
        bool: True if the line appears to be a header.
    """
    line_lower = line.lower()
    header_indicators = [
        "place", "athlete", "name", "school", "time", "mark", "heat", 
        "wind", "points", "grade", "year", "lane", "flight", "finals"
    ]
    
    # Count how many header indicators are present
    indicator_count = sum(1 for indicator in header_indicators if indicator in line_lower)
    
    # Consider it a header if it has multiple indicators and isn't a result line
    return (indicator_count >= 2 and 
            not re.search(r'^\s*\d+', line) and  # Doesn't start with a place number
            len(line.split()) >= 3)

def parse_result_line_with_columns(line: str, column_map: Dict[str, int]) -> Dict[str, str]:
    """
    Parse a result line using the identified column mapping by creating a dynamic regex pattern.
    
    Args:
        line (str): The result line to parse.
        column_map (dict): Column mapping from identify_column_order.
        
    Returns:
        dict: Extracted information from the line.
    """
    result = {
        "place": "",
        "name": "",
        "grade": "",
        "school": "",
        "mark": "",
        "heat": "",
        "wind": "",
        "points": "",
        "review": False
    }

    line = line.strip()
    if not line or line.startswith("=") or line.startswith("-"):
        result["review"] = True
        return result

    if len(line.split()) < 3:
        result["review"] = True
        return result

    # Column-specific regex patterns
    column_patterns = {
        "place": r'^(\d+|--?|DQ|DNS|DNF|NT)$',
        "name": r'^([\w\-\'\.]+(?:\s+[\w\-\'\.]+)*|[\w\-\'\.]+,\s*[\w\-\'\.]+)$',
        "grade": r'^(\d{1,2})$',
        "school": r'^[\w\s\-\'\.\/\(\)&]+$',
        "mark": r'^(\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2}|\d+\'\d{2}\.\d{2}"|\d+\-\d{2}\.\d{2}|NT|DQ|DNS|DNF)[q*#]?$',
        "heat": r'^(\d+)$',
        "wind": r'^[+-]?\d+\.\d+$',
        "points": r'^(\d+)$',
        "lane": r'^(\d+)$'
    }

    try:
        # Initial split using large spaces or tabs
        segments = re.split(r'\s{2,}|\t+', line)
        segments = [seg.strip() for seg in segments if seg.strip()]
        print(f"[DEBUG] Initial segments: {segments}")
        sorted_columns = sorted(column_map.items(), key=lambda x: x[1])
        expected_fields = [col for col, _ in sorted_columns]

        current_index = 0
        i = 0
        while i < len(expected_fields) and current_index < len(segments):
            col = expected_fields[i]
            pattern = column_patterns.get(col)
            segment = segments[current_index]

            if re.match(pattern, segment):
                result[col] = segment
                i += 1
                current_index += 1
            else:
                # Try to recover: split segment by space if possible
                sub_segments = segment.split()
                matched = False
                for j in range(1, len(sub_segments)):
                    candidate = " ".join(sub_segments[:j])
                    remainder = " ".join(sub_segments[j:])
                    if re.match(pattern, candidate):
                        result[col] = candidate
                        # Inject the remainder back for next column
                        segments[current_index] = remainder
                        matched = True
                        break
                if not matched:
                    # If recovery fails, mark as review and continue
                    print(f"[DEBUG] Could not match '{segment}' for column '{col}'")
                    result["review"] = True
                    break
                i += 1

        # Try to assign leftover segments
        leftovers = segments[current_index:]
        for col in expected_fields[i:]:
            if not leftovers:
                break
            segment = leftovers.pop(0)
            pattern = column_patterns.get(col)
            if re.match(pattern, segment):
                result[col] = segment
            else:
                result["review"] = True

        # Final validation
        if not result["place"] or not result["mark"]:
            result["review"] = True

    except Exception as e:
        print(f"[ERROR] parse_result_line_with_columns failed on line: {line} — {e}")
        result["review"] = True

    print(f"[DEBUG] Final parsed result: {result}")
    return result


def parse_with_fallback_logic(line: str, column_map: Dict[str, int]) -> Dict[str, str]:
    """
    Fallback parsing logic when dynamic pattern fails.
    
    Args:
        line (str): The result line to parse.
        column_map (dict): Column mapping from identify_column_order.
        
    Returns:
        dict: Extracted information from the line.
    """

    print("🚨 CALLING FALLBACK LOGIC 🚨")

    result = {
        "place": "",
        "name": "",
        "grade": "",
        "school": "",
        "mark": "",
        "heat": "",
        "wind": "",
        "points": "",
        "review": False
    }
    
    # Use the original hardcoded patterns as fallback
    patterns = [
        # Pattern 1: Place Name (Grade) School Mark Heat Wind Points
        r'^\s*(\d+|--?)\s+([\w\-\'\.\s,]+?)\s+(\d+)?\s+([\w\s\-\'\.\/\(\)]+?)\s+((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2}|\d+\'\d{2}\.\d{2}"|\d+\-\d{2}\.\d{2})[q*#]?)\s*(\d+)?\s*([+-]?\d+\.\d+)?\s*(\d+)?\s*$',
        
        # Pattern 2: Place Name School Mark (more flexible)
        r'^\s*(\d+|--?)\s+([\w\-\'\.\s,]+?)\s+([\w\s\-\'\.\/\(\)]+?)\s+((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2}|\d+\'\d{2}\.\d{2}"|\d+\-\d{2}\.\d{2})[q*#]?)\s*(.*)$',
        
        # Pattern 3: Team/relay format
        r'^\s*(\d+)\s+([\w\s\-\'\.\/\(\)]+?)\s+((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2})[q*#]?)\s*(\d+)?\s*(\d+)?\s*$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, line, re.IGNORECASE)
        if match:
            groups = match.groups()
            
            # Map groups to columns based on the standard patterns
            result["place"] = groups[0] if groups[0] else ""
            
            if len(groups) >= 4:
                # Check if this looks like a relay format
                if "RELAY" in line.upper() or len(groups) == 5:
                    # Relay format: Place Team Time Heat Points
                    result["school"] = groups[1].strip() if groups[1] else ""
                    result["mark"] = groups[2].strip() if groups[2] else ""
                    if len(groups) > 3 and groups[3]:
                        result["heat"] = groups[3].strip()
                    if len(groups) > 4 and groups[4]:
                        result["points"] = groups[4].strip()
                else:
                    # Individual format
                    result["name"] = groups[1].strip() if groups[1] else ""
                    
                    # Check if group 2 looks like a grade
                    if len(groups) >= 5 and groups[2] and groups[2].strip().isdigit() and int(groups[2].strip()) <= 12:
                        result["grade"] = groups[2].strip()
                        result["school"] = groups[3].strip() if groups[3] else ""
                        result["mark"] = groups[4].strip() if groups[4] else ""
                    else:
                        result["school"] = groups[2].strip() if groups[2] else ""
                        result["mark"] = groups[3].strip() if groups[3] else ""
                    
                    # Parse remaining fields
                    if len(groups) > 4 and groups[-1]:
                        remaining = groups[-1].strip().split()
                        for item in remaining:
                            if item.isdigit():
                                if not result["heat"]:
                                    result["heat"] = item
                                elif not result["points"]:
                                    result["points"] = item
                            elif re.match(r'^[+-]?\d+\.\d+$', item):
                                result["wind"] = item
            
            break
    else:
        result["review"] = True
    
    return result


def parse_result_line(line: str, column_map: Dict[str, int] = None) -> Dict[str, str]:
    """
    Parse a result line and extract relevant information.
    
    Args:
        line (str): The result line to parse.
        column_map (dict): Optional column mapping for structured parsing.
        
    Returns:
        dict: Extracted information from the line.
    """
    # If we have a column mapping, use it
    if column_map:
        return parse_result_line_with_columns(line, column_map)
    
    # Fall back to regex-based parsing
    result = {
        "place": "",
        "name": "",
        "grade": "",
        "school": "",
        "mark": "",
        "heat": "",
        "wind": "",
        "points": "",
        "review": False
    }
    
    # Clean the line
    line = line.strip()
    if not line or line.startswith("=") or line.startswith("-"):
        result["review"] = True
        return result
    
    # Try multiple parsing patterns
    patterns = [
        # Pattern 1: Place Name (Grade) School Mark Heat Wind Points
        r'^\s*(\d+|--?)\s+([\w\-\'\.\s,]+?)\s+(\d+)?\s+([\w\s\-\'\.\/\(\)]+?)\s+((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2}|\d+\'\d{2}\.\d{2}"|\d+\-\d{2}\.\d{2})[q*#]?)\s*(\d+)?\s*([+-]?\d+\.\d+)?\s*(\d+)?\s*$',
        
        # Pattern 2: Place Name School Mark (more flexible)
        r'^\s*(\d+|--?)\s+([\w\-\'\.\s,]+?)\s+([\w\s\-\'\.\/\(\)]+?)\s+((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2}|\d+\'\d{2}\.\d{2}"|\d+\-\d{2}\.\d{2})[q*#]?)\s*(.*)$',
        
        # Pattern 3: Team/relay format
        r'^\s*(\d+)\s+([\w\s\-\'\.\/\(\)]+?)\s+((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2})[q*#]?)\s*(\d+)?\s*(\d+)?\s*$'
    ]
    
    matched = False
    for pattern in patterns:
        match = re.match(pattern, line, re.IGNORECASE)
        if match:
            groups = match.groups()
            
            # Basic extraction
            result["place"] = groups[0] if groups[0] else ""
            
            if len(groups) >= 4:
                # Try to determine if this is a relay or individual event
                if "RELAY" in line.upper() or len(groups) == 5:
                    # Relay format: Place Team Time Heat Points
                    result["school"] = groups[1].strip() if groups[1] else ""
                    result["mark"] = groups[2].strip() if groups[2] else ""
                    if len(groups) > 3 and groups[3]:
                        result["heat"] = groups[3].strip() if groups[3] else ""
                    if len(groups) > 4 and groups[4]:
                        result["points"] = groups[4].strip() if groups[4] else ""
                else:
                    # Individual format
                    result["name"] = groups[1].strip() if groups[1] else ""
                    
                    # Determine which group is school vs grade
                    if len(groups) >= 5:
                        # Check if group 2 looks like a grade
                        if groups[2] and groups[2].strip().isdigit() and int(groups[2].strip()) <= 12:
                            result["grade"] = groups[2].strip() if groups[2].strip().isdigit() else ""
                            result["school"] = groups[3].strip() if groups[3] else ""
                            result["mark"] = groups[4].strip() if groups[4] else ""
                        else:
                            result["school"] = groups[2].strip() if groups[2] else ""
                            result["mark"] = groups[3].strip() if groups[3] else ""
                    else:
                        result["school"] = groups[2].strip() if groups[2] else ""
                        result["mark"] = groups[3].strip() if groups[3] else ""
                    
                    # Parse remaining fields from the end of the line
                    if len(groups) > 4 and groups[-1]:
                        remaining = groups[-1].strip().split() 
                        for item in remaining:
                            if item.isdigit():
                                if not result["heat"]:
                                    result["heat"] = item
                                elif not result["points"]:
                                    result["points"] = item
                            elif re.match(r'^[+-]?\d+\.\d+$', item):
                                result["wind"] = item
            
            matched = True
            break
    
    if not matched:
        result["review"] = True
    
    return result

def parse_results(file_path: str, metadata: Dict[str, str]) -> None:
    """
    Main function for parsing the track meet results and generate a structured CSV.
    
    Args:
        file_path (str): Path to the input text file.
        metadata (Dict[str, str]): Dictionary of constant metadata to include
            in each row that was inputted on web app upon file upload.
    """    
    # Define output columns
    columns = [
        "Meet Date", "Edition", "Meet Name", "Meet Location", "Season", "URL", 
        "Timing", "Event", "Round", "Gender", "Place", "Last Name", "First Name",
        "Grade", "School", "Mark", "Heat", "Wind", "Points", "Review"
    ]
    
    rows = []  # Store parsed data
    
    # Initialize variables to store current context
    current_event = ""
    current_gender = ""
    current_round = ""
    current_column_map = {}  # Store the column mapping for current event
    
    # Event detection patterns
    event_patterns = [
        re.compile(r"Event\s+\d+\s+(Boys|Girls)\s+(.+)", re.IGNORECASE),
        re.compile(r"(Boys?|Girls?)\s+(.+?)(?:\s+Division|\s+Finals|\s+Preliminaries|\s+Semi-Finals|$)", re.IGNORECASE),
        re.compile(r"^\s*(.+?)\s+(Boys?|Girls?)", re.IGNORECASE)
    ]
    
    # Distance events to potentially skip (if needed)
    distance_events = {"shot put", "discus", "high jump", "long jump", "triple jump", "pole vault", "javelin"}

    try:
        # Open file for reading
        with open(file_path, "r", encoding='utf-8', errors='ignore') as file:
            lines = file.readlines()

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip empty lines
            if not line:
                i += 1
                continue
            
            # Check for event detection
            event_detected = False
            for pattern in event_patterns:
                event_match = pattern.search(line)
                if event_match:
                    groups = event_match.groups()
                    
                    # Determine gender and event name based on pattern
                    if len(groups) == 2:
                        if groups[0].lower() in ['boys', 'boy']:
                            current_gender = "M"
                            raw_event_name = groups[1].strip()
                        elif groups[0].lower() in ['girls', 'girl']:
                            current_gender = "F"
                            raw_event_name = groups[1].strip()
                        else:
                            # Try the reverse
                            if groups[1].lower() in ['boys', 'boy']:
                                current_gender = "M"
                                raw_event_name = groups[0].strip()
                            elif groups[1].lower() in ['girls', 'girl']:
                                current_gender = "F"
                                raw_event_name = groups[0].strip()
                            else:
                                current_gender = extract_gender_from_event(line)
                                raw_event_name = line.strip()
                    else:
                        current_gender = extract_gender_from_event(line)
                        raw_event_name = line.strip()
                    
                    # Normalize event name
                    current_event, review_flag = normalize_event(raw_event_name, False)
                    current_round = ""  # Reset round
                    current_column_map = {}  # Reset column mapping for new event
                    event_detected = True
                    print(f"Event detected: {current_event}, Gender: {current_gender}")
                    break
            
            if event_detected:
                i += 1
                continue
            
            # Check for round information
            round_info = extract_round_from_line(line)
            if round_info:
                current_round = round_info
                print(f"Round detected: {current_round}")
                i += 1
                continue
            
            # Check if this is a header line and identify columns
            if is_header_line(line):
                current_column_map = identify_column_order(line)
                print(f"Header detected, column mapping updated: {current_column_map}")
                i += 1
                continue
            
            # Skip lines that are clearly headers or dividers
            if (line.startswith("=") or line.startswith("-") or 
                "COMPLETE RESULTS" in line or "Page" in line or
                line.lower().startswith("event") or
                len(line.split()) < 3):
                i += 1
                continue
            
            # Try to parse as result line
            if current_event:
                result = parse_result_line(line, current_column_map if current_column_map else None)
                
                if not result["review"] and (result["name"] or result["school"]):
                    # Parse name if present
                    last_name, first_name = "", ""
                    if result["name"]:
                        last_name, first_name = parse_name(result["name"])
                    
                    # Normalize school name
                    normalized_school = result["school"]
                    review_flag = result["review"]
                    if result["school"]:
                        normalized_school, review_flag = normalize_school(result["school"].strip(), review_flag)
                    
                    # Create row
                    row = {
                        "Event": current_event,
                        "Round": current_round or "Final",
                        "Gender": current_gender,
                        "Place": result["place"],
                        "Last Name": last_name,
                        "First Name": first_name,
                        "Grade": result["grade"],
                        "School": normalized_school,
                        "Mark": result["mark"],
                        "Heat": result["heat"],
                        "Wind": result["wind"],
                        "Points": result["points"],
                        "Review": review_flag or result["review"]
                    }
                    
                    # Add metadata
                    row.update(metadata)
                    rows.append(row)
                    print(f"Row added: {result['place']} - {result['name']} - {result['school']} - {result['mark']} - Heat: {result['heat']}")
            
            i += 1

    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing file: {e}")
        sys.exit(1)
    
    # Write output to CSV
    output_file = "output.csv"
    try:
        with open(output_file, "w", newline="", encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        
        print(f"Processed {len(rows)} results saved to {output_file}")
    except Exception as e:
        print(f"Error writing CSV file: {e}")
        sys.exit(1)