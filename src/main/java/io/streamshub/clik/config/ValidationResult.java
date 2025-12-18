package io.streamshub.clik.config;

public class ValidationResult {
    private final boolean valid;
    private final String message;

    private ValidationResult(boolean valid, String message) {
        this.valid = valid;
        this.message = message;
    }

    public static ValidationResult success() {
        return new ValidationResult(true, null);
    }

    public static ValidationResult failure(String message) {
        return new ValidationResult(false, message);
    }

    public boolean isValid() {
        return valid;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return valid ? "Valid" : "Invalid: " + message;
    }
}
