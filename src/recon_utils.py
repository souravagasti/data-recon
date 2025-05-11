def validate_settings(settings, required_keys, label="source"):
    """Validate that required keys exist in a settings dictionary."""
    missing = [key for key in required_keys if key not in settings]
    if missing:
        raise ValueError(f"Missing keys in {label} settings: {', '.join(missing)}")


def infer_excel_range(skip_rows, num_columns):
    """Generates a spreadsheet range like A4:J based on skip_rows and number of columns."""
    def col_letter(n):
        result = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            result = chr(65 + remainder) + result
        return result

    start_row = skip_rows + 1  # e.g., skip 3 means start from row 4
    end_col_letter = col_letter(num_columns)
    return f"A{start_row}:{end_col_letter}"
