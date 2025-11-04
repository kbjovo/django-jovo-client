# Generated migration for encrypting existing passwords

from django.db import migrations


def encrypt_existing_passwords(apps, schema_editor):
    """
    Encrypt all existing plaintext passwords in ClientDatabase.
    """
    from client.encryption import encrypt_password

    ClientDatabase = apps.get_model('client', 'ClientDatabase')

    for db_conn in ClientDatabase.objects.all():
        if db_conn.password:
            # Check if password is already encrypted (simple heuristic)
            # Encrypted passwords are base64-encoded Fernet tokens
            if len(db_conn.password) < 50 or 'gAAAAA' not in db_conn.password:
                # Password is not encrypted, encrypt it
                try:
                    encrypted = encrypt_password(db_conn.password)
                    # Use update to bypass the save() method
                    ClientDatabase.objects.filter(pk=db_conn.pk).update(password=encrypted)
                    print(f"Encrypted password for connection: {db_conn.connection_name}")
                except Exception as e:
                    print(f"Error encrypting password for {db_conn.connection_name}: {e}")


def decrypt_passwords(apps, schema_editor):
    """
    Reverse migration: decrypt all passwords back to plaintext.
    WARNING: This should only be used in development!
    """
    from client.encryption import decrypt_password

    ClientDatabase = apps.get_model('client', 'ClientDatabase')

    for db_conn in ClientDatabase.objects.all():
        if db_conn.password:
            try:
                decrypted = decrypt_password(db_conn.password)
                ClientDatabase.objects.filter(pk=db_conn.pk).update(password=decrypted)
                print(f"Decrypted password for connection: {db_conn.connection_name}")
            except Exception as e:
                print(f"Error decrypting password for {db_conn.connection_name}: {e}")


class Migration(migrations.Migration):

    dependencies = [
        ('client', '0006_alter_client_db_name_alter_client_email_and_more'),
    ]

    operations = [
        migrations.RunPython(encrypt_existing_passwords, decrypt_passwords),
    ]