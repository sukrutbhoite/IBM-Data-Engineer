export PGPASSWORD=05VLMw3qfGelDiDwLCWkDZ4A; psql --host 172.21.117.139 -p 5432 -U postgres -d COFFEE -c "\copy (SELECT * FROM product_info_m_view) TO 'product_info_m-view.csv' WITH (FORMAT CSV, HEADER);"


export PGPASSWORD=05VLMw3qfGelDiDwLCWkDZ4A; psql --host 172.21.117.139 -p 5432 -U postgres -d COFFEE -c "\copy (SELECT * FROM staff_locations_view) TO 'staff_locations_view.csv' WITH (FORMAT CSV, HEADER);"