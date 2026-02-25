from django.urls import path
from django.conf import settings
from rest_framework.routers import DefaultRouter
from rest_framework.routers import SimpleRouter

from voters.users.api.views import UserViewSet, UserRegisterationView, UserLoginTokenView

router = DefaultRouter() if settings.DEBUG else SimpleRouter()

router.register("users", UserViewSet)


app_name = "api"

urlpatterns = [
    path("register/", UserRegisterationView.as_view(), name="register"),
    path("login/", UserLoginTokenView.as_view(), name="login"),

]
urlpatterns += router.urls
