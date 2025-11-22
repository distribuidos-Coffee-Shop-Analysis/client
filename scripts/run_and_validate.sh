#!/bin/bash
#
# Script to run a client and automatically validate results after completion
#
# Usage: ./run_and_validate.sh <client_num> [dataset_type]
#   client_num: 1, 2, 3, etc.
#   dataset_type: normal (default), short, example-1, example-2, etc.
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
CLIENT_NUM=${1:-1}
DATASET_TYPE=${2:-normal}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="$PROJECT_DIR/output/client_$CLIENT_NUM"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Client Execution ${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "Client ID: ${GREEN}client_$CLIENT_NUM${NC}"
echo -e "Dataset:   ${GREEN}$DATASET_TYPE${NC}"
echo -e "Output:    ${GREEN}$OUTPUT_DIR${NC}"
echo -e "${BLUE}========================================${NC}\n"

cd "$PROJECT_DIR"

case "$DATASET_TYPE" in
    short)
        CLIENT_NUM=$CLIENT_NUM make docker-compose-up-short
        ;;
    example-1)
        make example-1-up
        ;;
    example-2)
        make example-2-up
        ;;
    example-3)
        make example-3-up
        ;;
    example-4)
        make example-4-up
        ;;
    example-5)
        make example-5-up
        ;;
    normal|*)
        CLIENT_NUM=$CLIENT_NUM make docker-compose-up
        ;;
esac

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo -e "\n${RED}❌ Client execution failed with exit code $EXIT_CODE${NC}"
    exit $EXIT_CODE
fi

echo -e "\n${GREEN}Ejecución completada${NC}\n"

if [ ! -d "$OUTPUT_DIR" ]; then
    echo -e "${RED}❌ Output directory not found: $OUTPUT_DIR${NC}"
    exit 1
fi
# python3 scripts/sort_results.py answers_short
python3 "$SCRIPT_DIR/sort_results.py" "$OUTPUT_DIR"

if [ $? -ne 0 ]; then
    echo -e "\n${RED}❌ Failed to sort results${NC}"
    exit 1
fi

echo -e "\n${GREEN}Resultados ordenados correctamente${NC}\n"

if [ "$DATASET_TYPE" = "short" ]; then
    ANSWERS_DIR="$PROJECT_DIR/answers_short"
else
    ANSWERS_DIR="$PROJECT_DIR/answers"
fi

# python3 scripts/compare_results.py answers_short output/client_1
python3 "$SCRIPT_DIR/compare_results.py" "$ANSWERS_DIR" "$OUTPUT_DIR"

COMPARE_EXIT=$?

echo ""
echo -e "${BLUE}========================================${NC}"
if [ $COMPARE_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ Todos los resultados son los esperados${NC}"
else
    echo -e "${YELLOW}⚠️  Termina con diferencias${NC}"
fi
echo -e "${BLUE}========================================${NC}"

exit $COMPARE_EXIT
