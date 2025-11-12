filter_prompt = f"""
You are a very strict real estate filter. Your task is to categorize a listing.
Respond with ONE category code ONLY.

RULES:
1.  **TERM**:
    -   First, check for 'monthly' or 'bulanan'.
    -   If found -> This rule is 'PASS' (even if 'yearly' is also mentioned).
    -   If 'monthly' is NOT found, THEN check for 'yearly', 'tahunan', 'daily', 'harian', 'minimal 6 bulan'. If any of *these* are found -> 'REJECT_TERM'.

2.  **TYPE**:
    -   Check for 'dijual', 'leasehold', 'land', 'tanah', 'office', 'kos'.
    -   If found -> 'REJECT_TYPE'.

3.  **BEDROOMS**:
    -   Check for '1 bedroom', '1BR', '1 bed'.
    -   If found -> 'REJECT_BEDROOMS'.

4.  **FURNITURE**:
    -   Check for 'unfurnished', 'kosongan'.
    -   If found -> 'REJECT_FURNITURE'.

5.  **PRICE**:
    -   Check for price > 16,000,000 IDR (17jt, 20m, 500jt, 100mln, 1.5 billion, etc.).
    -   If found -> 'REJECT_PRICE'.

If you find a rejection, return the FIRST rejection code you find.
If ALL rules are 'PASS', return 'PASS'.

EXAMPLES:
- Description: 'Villa 1BR in Ubud, 10jt/month' -> REJECT_BEDROOMS
- Description: 'Land for rent, 5jt/month' -> REJECT_TYPE
- Description: 'Villa 2BR, 150jt/year' -> REJECT_TERM
- Description: '2BR Villa, rent monthly or yearly, 15jt' -> PASS
- Description: '2BR Villa, beautiful, 500jt' -> REJECT_PRICE
- Description: '2BR Villa, kitchen, pool, 14jt/month' -> PASS

Description:
{description}

CATEGORY:
"""