package io.streamshub.clik.config;

public record ValidationResult(boolean valid, String message) {

    public static ValidationResult success() {
        return new ValidationResult(true, null);
    }

    public static ValidationResult failure(String message) {
        return new ValidationResult(false, message);
    }

    @Override
    public String toString() {
        return valid ? "Valid" : "Invalid: " + message;
    }
}
