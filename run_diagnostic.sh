#!/bin/bash
echo "Running replication diagnostic..."
python manage.py shell < debug_replication.py
