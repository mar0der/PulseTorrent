#!/bin/bash
#
# PulseTorrent — Fix macOS "damaged" warning
#
# Double-click this file to fix the macOS security warning
# that prevents PulseTorrent from opening.
#
# What it does: removes the quarantine flag that macOS adds
# to apps downloaded from the internet when they aren't
# code-signed by an Apple Developer account.
#

APP_NAME="PulseTorrent"
APP_PATH="/Applications/${APP_NAME}.app"

clear
echo "======================================"
echo "  PulseTorrent — macOS Fix"
echo "======================================"
echo ""

if [ ! -d "$APP_PATH" ]; then
    echo "Could not find ${APP_NAME} at:"
    echo "  $APP_PATH"
    echo ""
    echo "Please drag PulseTorrent.app to your Applications"
    echo "folder first, then run this script again."
    echo ""
    read -n 1 -s -r -p "Press any key to close..."
    exit 1
fi

echo "This will fix the \"damaged\" warning for:"
echo "  $APP_PATH"
echo ""
echo "You may be asked for your password."
echo ""

xattr -cr "$APP_PATH"

if [ $? -eq 0 ]; then
    echo "Done! PulseTorrent has been fixed."
    echo ""
    echo "Opening PulseTorrent..."
    open "$APP_PATH"
else
    echo "Something went wrong. Try running this"
    echo "command manually in Terminal:"
    echo ""
    echo "  xattr -cr $APP_PATH"
fi

echo ""
read -n 1 -s -r -p "Press any key to close..."
