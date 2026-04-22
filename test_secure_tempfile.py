import tempfile
import os

tmp_dir = tempfile.gettempdir()
symbol = "../../../etc/passwd"
model_type = "lstm"

# Using NamedTemporaryFile ensures uniqueness and security against traversal
df_fd, df_path = tempfile.mkstemp(prefix=f'ml_tmp_df_', suffix='.pkl', dir=tmp_dir)
os.close(df_fd)

print(df_path)
