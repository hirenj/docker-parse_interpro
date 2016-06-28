FROM nodesource/wheezy:4.3.2

# cache package.json and node_modules to speed up builds
ADD package.json package.json
RUN npm install

ADD . .
CMD ["npm", "start"]