"""
Add this to client/utils/date_converter.py
"""
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def convert_integer_to_date(value):
    """
    Convert various integer date formats to MySQL DATE format (YYYY-MM-DD)
    
    Supported formats:
    - 20238 (Julian: YYDDD) -> Year 2020, Day 238
    - 230820 (YYMMDD) -> 2023-08-20
    - 20230820 (YYYYMMDD) -> 2023-08-20
    """
    if not isinstance(value, (int, str)):
        return None
    
    date_str = str(value).strip()
    
    try:
        # Format 1: YYYYMMDD (8 digits)
        if len(date_str) == 8:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            
            # Validate
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        
        # Format 2: YYMMDD (6 digits)
        elif len(date_str) == 6:
            year = 2000 + int(date_str[:2])
            month = date_str[2:4]
            day = date_str[4:6]
            
            # Validate
            datetime(year, int(month), int(day))
            return f"{year}-{month}-{day}"
        
        # Format 3: YYDDD (5 digits) - Julian Date
        elif len(date_str) == 5:
            year_part = int(date_str[:2])
            day_of_year = int(date_str[2:])
            
            # Assume 20xx for years >= 0
            year = 2000 + year_part
            
            # Convert day-of-year to date
            date_obj = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
            return date_obj.strftime('%Y-%m-%d')
        
        # Format 4: YYYYDDD (7 digits) - Full Julian Date
        elif len(date_str) == 7:
            year = int(date_str[:4])
            day_of_year = int(date_str[4:])
            
            date_obj = datetime(year, 1, 1) + timedelta(days=day_of_year - 1)
            return date_obj.strftime('%Y-%m-%d')
        
        else:
            logger.warning(f"Unknown date format (length={len(date_str)}): {value}")
            return None
            
    except (ValueError, OverflowError) as e:
        logger.error(f"Invalid date value {value}: {e}")
        return None


# # Quick test
# if __name__ == "__main__":
#     test_cases = [
#         20238,      # Julian: 2020, Day 238 -> 2020-08-25
#         230820,     # YYMMDD: 2023-08-20
#         20230820,   # YYYYMMDD: 2023-08-20
#         2023238,    # YYYYDDD: 2023, Day 238 -> 2023-08-26
#     ]
    
#     for test in test_cases:
#         result = convert_integer_to_date(test)
#         print(f"{test} -> {result}")