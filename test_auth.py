from services.auth_service import AuthService
auth = AuthService()
# let's try to authenticate newuser with password newuser
user = auth.authenticate("newuser", "newuser")
print("newuser:newuser =>", user)
