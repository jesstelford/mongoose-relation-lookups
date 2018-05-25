const {
  Schema,
  Types: { ObjectId }
} = (mongoose = require("mongoose"));

const uri = "mongodb://localhost/looktest";

mongoose.Promise = global.Promise;
mongoose.set("debug", true);

const userSchema = new Schema({
  name: String
});

const categorySchema = new Schema({
  name: String
});

const postSchema = new Schema(
  {
    title: String,
    author: { type: Schema.Types.ObjectId, ref: "User" },
    categories: [{ type: Schema.Types.ObjectId, ref: "Category" }],
  },
  { timestamps: true }
);

function getRelModel(model, path) {
}

postSchema.statics.lookup = function({ path, query }) {
  let rel;
  const many = this.schema.path(path).$isMongooseArray;
  if (many) {
    rel = mongoose.model(this.schema.path(path).caster.options.ref);
  } else {
    rel = mongoose.model(this.schema.path(path).options.ref);
  }

  // MongoDB 3.6 and up $lookup with sub-pipeline
  // FIXME: Not working
  console.log('rel.collection.name', rel.collection.name);

  return this.aggregate([
    {
      $lookup: {
        from: rel.collection.name,
        as: path,
        let: { [path]: `$${path}` },
        pipeline: [
          {
            $match: {
              ...query,
              $expr: { [many ? '$in' : '$eq']: ["$_id", `$$${path}`] }
            }
          }
        ]
      }
    },
    {
      $unwind: `$${path}`,
    }
  ])
    .exec()
    .then(data => {
      console.log(...data);
      return data.map(item =>
        this({
          ...item,
          [path]: Array.isArray(item[path]) ? item[path].map(data => rel(data)) : rel(item[path]),
        })
      );
    });
};

const User = mongoose.model("User", userSchema);
const Category = mongoose.model("Category", categorySchema);
const Post = mongoose.model("Post", postSchema);

const log = body => console.log(JSON.stringify(body, undefined, 2));

(async function() {
  try {
    const conn = await mongoose.connect(uri);

    // Clean data
    await Promise.all(Object.entries(conn.models).map(([k, m]) => m.remove()));

    // Create items
    const users = await User.insertMany(
      ["Jed Watson", "Jess Telford", "Boris Bozic"].map(name => ({ name }))
    );

    const categories = await Category.insertMany(
      ["React", "GraphQL", "node", "frontend"].map(name => ({ name }))
    );

    await Post.create({
      title: "Something",
      author: users[0],
      categories: [categories[0]],
    });

    await Post.create({
      title: "An Article",
      author: users[0],
      categories: [categories[0], categories[1]],
    });

    await Post.create({
      title: "And another thing!",
      author: users[1],
      categories: [categories[1], categories[2]],
    });

    await Post.create({
      title: "Oh hi there...",
      author: users[2],
      categories: [categories[2], categories[3]],
    });

    // Query with our static
    const postAuthorNames = [/Jess/i];
    let result = await Post.lookup({
      path: "author",
      query: { "name": { $in: postAuthorNames } }
    });
    console.log(`Lookup posts with Author ${postAuthorNames}`);
    log(result);

    // Query with our static
    const postCategoryNames = ['React', 'frontend'];
    result = await Post.lookup({
      path: "categories",
      query: { "name": { $in: postCategoryNames } }
    });
    console.log(`Lookup posts with Categories ${postCategoryNames}`);
    log(result);

    await mongoose.disconnect();
  } catch (e) {
    console.error(e);
  } finally {
    process.exit();
  }
})();
