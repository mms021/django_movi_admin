from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _
from django.db import models
import uuid


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    """
    Опсиание модели: Жанр.
    """
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = "content\".\"genre"
        verbose_name = 'genre'
        verbose_name_plural = 'genres'


class Filmwork(UUIDMixin, TimeStampedMixin):
    """
    Опсиание модели: Фильм.
    """
    class Film_type(models.TextChoices):
        Movie = 'mv', _('movie')
        Tv_show = 'tv', _('tv_show')

    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True)
    creation_date = models.DateField(_('creation_date'), blank=True)
    rating = models.FloatField(_('rating'), blank=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.CharField(_('type'), max_length=2,
                            choices=Film_type.choices,
                            default=Film_type.Movie)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    certificate = models.CharField(_('certificate'),
                                   max_length=512, blank=True,null=True,)
    file_path = models.FileField(_('file'), blank=True,
                                 null=True, upload_to='movies/')

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"film_work"
        verbose_name = 'film_work'
        verbose_name_plural = 'film_works'


class GenreFilmwork(UUIDMixin):
    """
    Опсиание модели: Жанр фильма.
    """
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.title

    class Meta:
        db_table = "content\".\"genre_film_work"


class Person(UUIDMixin, TimeStampedMixin):
    """
    Опсиание модели: Актер.
    """
    class Gender(models.TextChoices):
        MALE = 'male', _('male')
        FEMALE = 'female', _('female')

    full_name = models.TextField(_('full_name'))
    gender = models.TextField(_('gender'), choices=Gender.choices, null=True)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = "content\".\"person"
        verbose_name = 'Person'
        verbose_name_plural = 'Persons'


class PersonFilmWork(UUIDMixin):
    """
    Опсиание модели: Актер фильма.
    """
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    role = models.CharField(_('role'), max_length=120, null=True)
    created = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.person.full_name

    class Meta:
        db_table = "content\".\"person_film_work"
        verbose_name = 'Person_film_work'
        verbose_name_plural = 'Persons_film_works'
