"""
Surname Extraction Utility

Extracts surname (last name) from Nepali full names.
Handles edge cases like multiple surnames, single names, special characters.
"""

import re


def extract_surname(full_name):
    """
    Extract surname from Nepali full name.
    
    Logic:
    1. Clean the name (remove extra spaces, special chars)
    2. Split by space
    3. Take the last word as surname
    4. Handle edge cases
    
    Args:
        full_name (str): Full name in Nepali (e.g., "राम बहादुर थापा")
    
    Returns:
        str: Extracted surname (e.g., "थापा")
    
    Examples:
        >>> extract_surname("राम बहादुर थापा")
        'थापा'
        
        >>> extract_surname("अनिता के.सी.")
        'के.सी.'
        
        >>> extract_surname("सीता")  # Single name
        'सीता'
    """
    
    if not full_name or not isinstance(full_name, str):
        return ''
    
    # Remove extra whitespace and strip
    cleaned_name = ' '.join(full_name.split())
    
    # Handle empty after cleaning
    if not cleaned_name:
        return ''
    
    # Split by space
    parts = cleaned_name.split()
    
    # If only one word, return it as surname
    if len(parts) == 1:
        return parts[0]
    
    # Return last word as surname
    surname = parts[-1]
    
    # Clean up any trailing punctuation (except dots which are part of abbreviations)
    surname = surname.rstrip(',;:!')
    
    return surname


def normalize_surname(surname):
    """
    Normalize surname for consistent matching.
    
    Handles variations like:
    - बुढाथोकी / वुढाथोकी
    - वि.क. / बि.क.
    
    Args:
        surname (str): Surname to normalize
    
    Returns:
        str: Normalized surname
    """
    
    if not surname:
        return ''
    
    # Convert to lowercase for comparison (if using English)
    normalized = surname.strip()
    
    # Common variations mapping (can be expanded)
    variations = {
        'वुढाथोकी': 'बुढाथोकी',
        'बि.क.': 'वि.क.',
        'बोहरा': 'वोहरा',
    }
    
    # Check if surname has a known variation
    if normalized in variations:
        return variations[normalized]
    
    return normalized


def validate_name(name):
    """
    Validate if the name is properly formatted.
    
    Args:
        name (str): Name to validate
    
    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    
    if not name or not isinstance(name, str):
        return False, "Name is required"
    
    cleaned = name.strip()
    
    if len(cleaned) < 2:
        return False, "Name is too short"
    
    if len(cleaned) > 200:
        return False, "Name is too long"
    
    # Check if name contains at least one Nepali or English character
    if not re.search(r'[\u0900-\u097Fa-zA-Z]', cleaned):
        return False, "Name must contain valid characters"
    
    return True, None


# Example usage and testing
if __name__ == '__main__':
    # Test cases
    test_names = [
        "राम बहादुर थापा",
        "अनिता के.सी.",
        "सीता",
        "मिना कुमारी बुढाथोकी रोका",
        "अङना चौधरी",
    ]
    
    print("Surname Extraction Tests:")
    print("-" * 50)
    for name in test_names:
        surname = extract_surname(name)
        print(f"{name:30} → {surname}")