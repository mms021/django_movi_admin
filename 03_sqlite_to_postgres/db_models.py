import uuid
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class Genre:
    id: uuid.UUID
    name: str
    description: str

    def tupl_str(self) -> str:
        return "('%s', '%s', '%s')" % (self.id, self.name, self.description)


@dataclass(frozen=True)
class GenreFW:
    id: uuid.UUID
    film_work: uuid.UUID
    genre: str
    created: date

    def tupl_str(self) -> str:
        return "('%s', '%s', '%s', '%s')" % (self.id, self.film_work,
                                             self.genre, self.created)


@dataclass(frozen=True)
class PersonFW:
    id: uuid.UUID
    film_work: uuid.UUID
    person: uuid.UUID
    role: str
    created: date

    def tupl_str(self) -> str:
        return "('%s', '%s', '%s', '%s', '%s')" % (self.id, self.film_work,
                                                   self.person, self.role,
                                                   self.created)


@dataclass(frozen=True)
class Person:
    id: uuid.UUID
    full_name: str
    gender: str

    def tupl_str(self) -> str:
        return "('%s', $$%s$$, '%s')" % (self.id, self.full_name, self.gender)


@dataclass(frozen=True)
class FilmW:
    id: uuid.UUID
    title: str
    description: str
    creation: date
    rating: float
    type: str

    def tupl(self) -> tuple:
        return (self.id, self.title, self.description, self.rating, self.type)
