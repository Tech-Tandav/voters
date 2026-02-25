from channels.middleware import BaseMiddleware


class TokenAuthMiddleware(BaseMiddleware):
    async def __call__(self, scope, receive, send):
        from backend.calculate.sockets.utils import get_user_from_token
        from django.contrib.auth.models import AnonymousUser
        from rest_framework.authtoken.models import Token
        # Expect token in query string: ?token=abcd1234
        query_string = scope["query_string"].decode()
        token_key = None
        if "token=" in query_string:
            token_key = query_string.split("token=")[-1]
        if token_key:
            try:
                token = await Token.objects.aget(key=token_key)
                scope["user"] = await get_user_from_token(token)
            except Exception:
                scope["user"] = AnonymousUser()
        else:
            scope["user"] = AnonymousUser()
   
        return await super().__call__(scope, receive, send)
