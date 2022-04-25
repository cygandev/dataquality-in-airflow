# create test directories
export SODA_DIR='dags/soda'
export DATASOURCES_YML_PATH='dags/soda/datasources.yml'

for dir in `python3 ./utils/dir_creator.py ${DATASOURCES_YML_PATH}`; do \
    mkdir -p ./${SODA_DIR}/$dir; \
done
# End