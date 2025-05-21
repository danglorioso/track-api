#!/usr/bin/env python
import re
import csv
import io
import sys
from typing import List, Dict
from rapidfuzz import fuzz, process

from standard_events import STANDARD_EVENTS
from standard_schools import STANDARD_SCHOOLS

def normalize_event(event_name: str, review_bool: bool) -> str:
    """
    Normalize the event name to the closest standardized name using fuzzy matching.
    
    Args:
        event_name (str): The event name to normalize.
        review_bool (bool): The review flag to mark if the event name is missing or invalid.
    
    Returns:
        str: The standardized event name if a match is found, otherwise the original name.
    """

    # Initialize best match and score variables
    best_match = ""
    best_score = 0

    for standard_event, alternatives in STANDARD_EVENTS.items():
        # Combine standard event and alternatives for fuzzy matching
        possible_matches = [standard_event] + alternatives
        result = process.extractOne(event_name, possible_matches)

        # If score is higher than previous best, update with standardized
        # event name and score
        if result[1] > best_score:
            best_match, best_score = standard_event, result[1]

    # At end of loop, return best match if score is above threshold (85)
    if best_score > 85:
        print(best_match)
        return best_match, review_bool
    else :
        # Return original if no close match found
        return event_name, True

def normalize_school(school_name: str, review_bool: bool) -> str:
    """
    Normalize the school name to the closest standardized name using fuzzy matching.
    
    Args:
        school_name (str): The school name to normalize.
        review_bool (bool): The review flag to mark if the school name is missing or invalid.
    
    Returns:
        str: The standardized school name if a match is found, otherwise the original name.
    """
    # Initialize best match and score variables
    bset_match = ""
    best_score = 0

    for standard_school, alternatives in STANDARD_SCHOOLS.items():
        # Combine standard school and alternatives for fuzzy matching
        possible_matches = [standard_school] + alternatives
        result = process.extractOne(school_name, possible_matches, scorer=fuzz.partial_ratio)

        # If score is higher than previous best, update with standardized 
        # school name and score
        if result[1] > best_score:
            best_match, best_score = standard_school, result[1]

    # At end of loop, return best match if score is above threshold (85)
    if best_score > 85:
        return best_match, review_bool
    else:
        # Return original if no close match found
        return school_name, True

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

    # Patterns for parsing
    event_pattern = re.compile(r"Event\s+\d+\s+(Boys|Girls)\s+(.+)")
    result_pattern = re.compile(
        r"(\d+)\s+"                              # Place number (integer)
        r"([\w\-\'\.]+(?:\s[\w\-\'\.]+){0,2})\s+"  # Name (first and last, up to three words)
        r"(\d+)?\s+"                             # Grade level (optional integer)
        r"([\w\s\-\'\.]+?)\s+"                   # School (string, non-greedy to stop at "mark")
        r"((?:\d{1,2}:\d{2}\.\d{2}|\d{1,2}\.\d{2})[q*]?)\s+"  # Mark (##.## or #:##.##, optionally ending in 'q' or '*')
        r"(\d+)?\s*"                             # Heat (optional integer)
        r"(\d+\.\d+)?\s*"                        # Optional unrounded times (decimal)
        r"(\d+)?\s*"                             # Optional wind/points numbers (integer)
    )

    finals_pattern = re.compile(r"Finals")
    gender_map = {"Girls": "F", "Boys": "M"}
    
    # Define distance events
    distance_events = {"shot put", "discus", "high jump", "long jump", "triple jump", "pole vault", "javelin"}

    # Initialize variables to store current
    current_event = ""
    current_gender = ""
    current_round = ""

    try:
        # Open file for reading
        with open(file_path, "r") as file:

            # Iterate through each line in the file
            for line in file:
                # Reset review flag
                review_bool = False

                # Check if line specifies event
                event_match = event_pattern.search(line)

                # Event detected
                if event_match:

                    # Extract gender from event line
                    current_gender = gender_map[event_match.group(1)]

                    # Extract and normalize event name
                    raw_event_name = event_match.group(2).strip()
                    current_event, review_bool = normalize_event(raw_event_name, review_bool)
                    
                    # Skip parsing if the event is a distance event
                    if any(event in raw_event_name for event in distance_events):
                        print(f"Skipping distance event: {current_event}")
                        current_event = None  # Clear current event for skipped events
                        continue
                    
                    continue
                
                # Detect finals round
                if finals_pattern.search(line):
                    current_round = "Final"
                    continue
                
                # Match individual results
                result_match = result_pattern.search(line)

                # Result detected
                if result_match:
                    # Extract result fields
                    place, full_name, grade, school, mark, heat, wind, points = result_match.groups()

                    # Parse full name into last and first names
                    if len(full_name.split()) > 1:
                        last_name, first_name = parse_name(full_name)

                    print(f"Place: {place}, Name: {full_name}, Grade: {grade}, School: {school}, Mark: {mark}, Heat: {heat}, Wind: {wind}, Points: {points}")

                    # Normalize school name
                    normalized_school, review_bool = normalize_school(school.strip(), review_bool)
                    
                    # Append row to results
                    rows.append({   
                        "Event": current_event,
                        "Round": current_round or "Prelim",
                        "Gender": current_gender,
                        "Place": place,
                        "Last Name": last_name,
                        "First Name": first_name,
                        "Grade": grade or "",
                        "School": normalized_school,
                        "Mark": mark,
                        "Heat": heat or "",
                        "Wind": wind or "",
                        "Points": points or "",
                        "Review": review_bool
                    })
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        sys.exit(1)

    # Add metadata to each row
    for row in rows:
        row.update(metadata)
    
    # Write output to CSV
    output_file = "output.csv"
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Processed results saved to {output_file}")

def parse_name(full_name: str):
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
        last_name, first_name = full_name.split(',', 1)
        return last_name.strip(), first_name.strip()
    else:
        # Split by spaces for first name last name format
        parts = full_name.split()

        # Return the first name as the first word and last name as everything else
        if len(parts) > 1:
            return " ".join(parts[1:]), parts[0]

# if __name__ == "__main__":
#     '''
#     Main function to parse the results file.
#     '''

#     # Check for file path argument
#     if len(sys.argv) < 2:
#         print("Usage: python3 parse_file.py <results_file>")
#         sys.exit(1)
    
#     # Example metadata
#     metadata = {
#         "Meet Date": "2023-01-01",
#         "Meet Location": "Sample Location",
#         "Edition": "1st",
#         "Meet Name": "Sample Meet",
#         "Timing": "FAT",
#         "URL": "http://example.com",
#         "Season": "Indoor"
#     }

#     # Call main function to parse results
#     parse_results(sys.argv[1], metadata)
