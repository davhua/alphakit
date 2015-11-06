#!/usr/bin/python

PROJ_DIR = dirname(dirname(__file__))

def get_path(*path):
	return join(PROJ_DIR, *path)