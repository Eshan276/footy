from kivy.app import App
from kivy.uix.widget import Widget


class PongGame(Widget):
    pass


class myApp(App):
    def build(self):
        return PongGame()


if __name__ == '__main__':
    myApp().run()