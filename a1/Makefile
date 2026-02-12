# Makefile for MinHash/LSH Assignment
# Updated with unit tests and improved integration testing

.PHONY: all compile unit-test integration-test test clean help

# Default target: run all tests
all: test

# Compile the project
compile:
	@echo "Compiling project..."
	sbt compile

# Run unit tests (fast, ~200ms)
unit-test:
	@echo "Running unit tests..."
	sbt test

# Run integration tests (slower, but validates end-to-end)
integration-test:
	@echo "Running integration tests..."
	@./test-runner.sh

# Run both unit and integration tests
test: unit-test integration-test
	@echo ""
	@echo "✅ All tests passed!"

# Individual integration test targets (legacy compatibility)
test01:
	@./data/test-01.sh > /tmp/test-01.output
	@diff data/test-01.expected /tmp/test-01.output

test02:
	@./data/test-02.sh > /tmp/test-02.output
	@diff data/test-02.expected /tmp/test-02.output

test03:
	@./data/test-03.sh > /tmp/test-03.output
	@diff data/test-03.expected /tmp/test-03.output

test04:
	@./data/test-04.sh > /tmp/test-04.output
	@diff data/test-04.expected /tmp/test-04.output

test05:
	@./data/test-05.sh > /tmp/test-05.output
	@diff data/test-05.expected /tmp/test-05.output

test06:
	@./data/test-06.sh > /tmp/test-06.output
	@diff data/test-06.expected /tmp/test-06.output

test07:
	@./data/test-07.sh > /tmp/test-07.output
	@diff data/test-07.expected /tmp/test-07.output

# Quick test: run just one integration test
quick-test: test01

# Run with verbose diff output
test-verbose:
	@./test-runner.sh --diff

# Generate expected outputs (use with caution!)
expected:
	@echo "Regenerating expected outputs..."
	@./data/test-01.sh > data/test-01.expected
	@./data/test-02.sh > data/test-02.expected
	@./data/test-03.sh > data/test-03.expected
	@./data/test-04.sh > data/test-04.expected
	@./data/test-05.sh > data/test-05.expected
	@./data/test-06.sh > data/test-06.expected
	@./data/test-07.sh > data/test-07.expected
	@echo "✓ Expected outputs regenerated"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	sbt clean
	@rm -f /tmp/test-*.output /tmp/test-*.results /tmp/test-*.expected-results
	@echo "✓ Clean complete"

# Package for distribution
zip: dataFiles.txt
	@rm -f data.zip
	@zip data.zip -@ < dataFiles.txt
	@cp -pf data.zip /tmp/
	@echo "✓ Created data.zip"

# Show help
help:
	@echo "MinHash/LSH Assignment - Makefile Targets"
	@echo ""
	@echo "Testing:"
	@echo "  make test             - Run all tests (unit + integration)"
	@echo "  make unit-test        - Run unit tests only (fast, ~200ms)"
	@echo "  make integration-test - Run integration tests only (~40s)"
	@echo "  make test-verbose     - Run integration tests with diff output"
	@echo "  make quick-test       - Run just test01 (fastest check)"
	@echo "  make test01..test07   - Run individual integration test"
	@echo ""
	@echo "Development:"
	@echo "  make compile          - Compile the project"
	@echo "  make clean            - Clean build artifacts"
	@echo ""
	@echo "Utilities:"
	@echo "  make expected         - Regenerate expected outputs (use carefully!)"
	@echo "  make zip              - Package data files"
	@echo "  make help             - Show this help"
	@echo ""
	@echo "Examples:"
	@echo "  make                  # Run all tests"
	@echo "  make unit-test        # Quick validation during development"
	@echo "  make test01           # Run a single integration test"
